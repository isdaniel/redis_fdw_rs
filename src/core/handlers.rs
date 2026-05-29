use crate::{
    core::{
        column_utils::{
            compute_pushdown_column_index, datum_to_text_string, detect_ttl_column,
            extract_column_names, extract_delete_key, state_from_ptr, transform_insert_data,
            validate_column_count,
        },
        explain::{explain_foreign_modify, explain_foreign_scan},
        schema_import::{analyze_foreign_table, import_foreign_schema},
        state_manager::{extract_static_prefix, validate_key_prefix, RedisFdwState},
        truncate::exec_foreign_truncate,
    },
    join::{
        foreign_join::execute_foreign_join,
        planner::{
            add_parameterized_paths, begin_foreign_join_scan, get_foreign_join_paths,
            plan_foreign_join,
        },
    },
    query::{limit::extract_limit_offset_info, pushdown::WhereClausePushdown},
    tables::types::RedisTableType,
    utils::{helpers::*, memory::create_wrappers_memctx, row::Row},
};
use pgrx::{
    pg_sys::{Index, ModifyTable, PlannerInfo},
    prelude::*,
    PgMemoryContexts, PgRelation,
};
use std::ptr;

const REDISMODY: &str = "__redis_UD_key_name";

pub type FdwRoutine<A = AllocatedByRust> = PgBox<pgrx::pg_sys::FdwRoutine, A>;

#[pg_extern(create_or_replace)]
pub extern "C" fn redis_fdw_handler() -> FdwRoutine {
    log!("---> redis_fdw_handler");
    unsafe {
        let mut fdw_routine = PgBox::<pgrx::pg_sys::FdwRoutine, AllocatedByRust>::alloc_node(
            pgrx::pg_sys::NodeTag::T_FdwRoutine,
        );

        fdw_routine.GetForeignRelSize = Some(get_foreign_rel_size);
        fdw_routine.GetForeignPaths = Some(get_foreign_paths);
        fdw_routine.GetForeignPlan = Some(get_foreign_plan);

        // scan phase
        fdw_routine.BeginForeignScan = Some(begin_foreign_scan);
        fdw_routine.IterateForeignScan = Some(iterate_foreign_scan);
        fdw_routine.ReScanForeignScan = Some(re_scan_foreign_scan);
        fdw_routine.EndForeignScan = Some(end_foreign_scan);
        fdw_routine.RecheckForeignScan = Some(recheck_foreign_scan);
        fdw_routine.ShutdownForeignScan = Some(shutdown_foreign_scan);

        // explain
        fdw_routine.ExplainForeignScan = Some(explain_foreign_scan);
        fdw_routine.ExplainForeignModify = Some(explain_foreign_modify);

        // modify
        fdw_routine.AddForeignUpdateTargets = Some(add_foreign_update_targets);
        fdw_routine.PlanForeignModify = Some(plan_foreign_modify);
        fdw_routine.BeginForeignModify = Some(begin_foreign_modify);
        fdw_routine.ExecForeignInsert = Some(exec_foreign_insert);
        fdw_routine.ExecForeignDelete = Some(exec_foreign_delete);
        fdw_routine.ExecForeignUpdate = Some(exec_foreign_update);
        fdw_routine.EndForeignModify = Some(end_foreign_modify);
        fdw_routine.IsForeignRelUpdatable = Some(is_foreign_rel_updatable);

        // batch insert
        fdw_routine.ExecForeignBatchInsert = Some(exec_foreign_batch_insert);
        fdw_routine.GetForeignModifyBatchSize = Some(get_foreign_modify_batch_size);

        // COPY FROM / INSERT SELECT support
        fdw_routine.BeginForeignInsert = Some(begin_foreign_insert);
        fdw_routine.EndForeignInsert = Some(end_foreign_insert);

        // truncate
        fdw_routine.ExecForeignTruncate = Some(exec_foreign_truncate);

        // import schema
        fdw_routine.ImportForeignSchema = Some(import_foreign_schema);

        // analyze
        fdw_routine.AnalyzeForeignTable = Some(analyze_foreign_table);

        // join pushdown (FDW-to-FDW on same Redis server)
        fdw_routine.GetForeignJoinPaths = Some(get_foreign_join_paths);

        fdw_routine
    }
}

#[pg_guard]
extern "C-unwind" fn get_foreign_rel_size(
    _root: *mut pgrx::pg_sys::PlannerInfo,
    baserel: *mut pgrx::pg_sys::RelOptInfo,
    foreigntableid: pgrx::pg_sys::Oid,
) {
    log!("---> get_foreign_rel_size");
    unsafe {
        let ctx_name = format!("Wrappers_scan_{}", foreigntableid.to_u32());
        log!("Creating memory context: {}", ctx_name);
        let ctx = create_wrappers_memctx(&ctx_name);
        let mut state = RedisFdwState::new(ctx);

        let options = get_foreign_table_options(foreigntableid);
        log!("Foreign table options: {:?}", options);
        state.update_from_options(options);

        if let Some(table_type_str) = state.opts.get("table_type") {
            state.table_type = RedisTableType::from_str(table_type_str);
        }

        let rel = pg_sys::relation_open(foreigntableid, pg_sys::AccessShareLock as i32);
        state.ttl_column_index = detect_ttl_column((*rel).rd_att);
        pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);

        if let Err(e) = state.init_redis_connection_from_options() {
            log!(
                "Could not connect to Redis for cost estimation, using defaults: {}",
                e
            );
        }

        let cost_estimate = state.estimate_costs();
        log!(
            "Cost estimation: rows={}, startup_cost={}, total_cost={}, width={}",
            cost_estimate.rows,
            cost_estimate.startup_cost,
            cost_estimate.total_cost,
            cost_estimate.width
        );

        state.redis_connection = None;

        let estimated_rows = cost_estimate.rows;
        state.cost_estimate = Some(cost_estimate);

        let state_ptr = PgMemoryContexts::For(ctx).leak_and_drop_on_delete(state);
        (*baserel).fdw_private = state_ptr as *mut std::os::raw::c_void;
        (*baserel).rows = estimated_rows;
    }
}

#[pg_guard]
extern "C-unwind" fn get_foreign_paths(
    _root: *mut pgrx::pg_sys::PlannerInfo,
    baserel: *mut pgrx::pg_sys::RelOptInfo,
    _foreigntableid: pgrx::pg_sys::Oid,
) {
    log!("---> get_foreign_paths");
    unsafe {
        let state_ptr = (*baserel).fdw_private as *mut RedisFdwState;
        let (startup_cost, total_cost) = if !state_ptr.is_null() {
            let state = &*state_ptr;
            if let Some(ref estimate) = state.cost_estimate {
                log!(
                    "Using calculated costs: startup={}, total={}",
                    estimate.startup_cost,
                    estimate.total_cost
                );
                (estimate.startup_cost, estimate.total_cost)
            } else {
                log!("No cached estimate, using fallback costs");
                (10.0, (*baserel).rows * 0.1 + 10.0)
            }
        } else {
            log!("State not available, using default costs");
            (10.0, 100.0)
        };

        let path = pgrx::pg_sys::create_foreignscan_path(
            _root,
            baserel,
            ptr::null_mut(),
            (*baserel).rows,
            #[cfg(feature = "pg18")]
            0,
            startup_cost,
            total_cost,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            #[cfg(any(feature = "pg17", feature = "pg18"))]
            ptr::null_mut(),
            ptr::null_mut(),
        );
        pgrx::pg_sys::add_path(baserel, path as *mut pgrx::pg_sys::Path);

        if !state_ptr.is_null() {
            let state = &*state_ptr;
            let supports_param = match &state.table_type {
                RedisTableType::Hash(_) | RedisTableType::Set(_) | RedisTableType::ZSet(_) => {
                    !state.is_multi_key
                }
                RedisTableType::String(_) => state.is_multi_key,
                _ => false,
            };
            if supports_param {
                add_parameterized_paths(_root, baserel, state);
            }
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn get_foreign_plan(
    root: *mut pgrx::pg_sys::PlannerInfo,
    baserel: *mut pgrx::pg_sys::RelOptInfo,
    foreigntableid: pgrx::pg_sys::Oid,
    best_path: *mut pgrx::pg_sys::ForeignPath,
    tlist: *mut pgrx::pg_sys::List,
    scan_clauses: *mut pgrx::pg_sys::List,
    outer_plan: *mut pgrx::pg_sys::Plan,
) -> *mut pgrx::pg_sys::ForeignScan {
    log!("---> get_foreign_plan");

    if (*baserel).reloptkind == pg_sys::RelOptKind::RELOPT_JOINREL {
        return plan_foreign_join(root, baserel, best_path, tlist, scan_clauses, outer_plan);
    }

    let state = state_from_ptr((*baserel).fdw_private);

    let is_parameterized = !(*best_path).path.param_info.is_null();
    let mut fdw_exprs: *mut pg_sys::List = ptr::null_mut();
    let mut param_col_idx: usize = 0;

    if is_parameterized {
        let my_relids = (*baserel).relids;
        let clause_list = pg_sys::extract_actual_clauses(scan_clauses, false);
        let clause_len = pg_sys::list_length(clause_list);
        for i in 0..clause_len {
            let clause_node = pg_sys::list_nth(clause_list, i) as *mut pg_sys::Node;
            if clause_node.is_null() || (*clause_node).type_ != pg_sys::NodeTag::T_OpExpr {
                continue;
            }
            let op_expr = &*(clause_node as *mut pg_sys::OpExpr);
            if pg_sys::list_length(op_expr.args) != 2 {
                continue;
            }
            let left_arg = pg_sys::list_nth(op_expr.args, 0) as *mut pg_sys::Node;
            let right_arg = pg_sys::list_nth(op_expr.args, 1) as *mut pg_sys::Node;
            if left_arg.is_null() || right_arg.is_null() {
                continue;
            }
            if (*left_arg).type_ != pg_sys::NodeTag::T_Var
                || (*right_arg).type_ != pg_sys::NodeTag::T_Var
            {
                continue;
            }
            let left_var = &*(left_arg as *mut pg_sys::Var);
            let right_var = &*(right_arg as *mut pg_sys::Var);

            let left_is_mine = pg_sys::bms_is_member(left_var.varno as i32, my_relids);
            let right_is_mine = pg_sys::bms_is_member(right_var.varno as i32, my_relids);

            if left_is_mine && !right_is_mine && left_var.varattno > 0 {
                param_col_idx = (left_var.varattno - 1) as usize;
                state.param_type_oid = pg_sys::exprType(right_arg);
                fdw_exprs = pg_sys::lappend(fdw_exprs, right_arg as *mut std::ffi::c_void);
                break;
            } else if right_is_mine && !left_is_mine && right_var.varattno > 0 {
                param_col_idx = (right_var.varattno - 1) as usize;
                state.param_type_oid = pg_sys::exprType(left_arg);
                fdw_exprs = pg_sys::lappend(fdw_exprs, left_arg as *mut std::ffi::c_void);
                break;
            }
        }

        if !fdw_exprs.is_null() {
            state.is_parameterized = true;
            state.param_column = param_col_idx;
            log!(
                "Parameterized scan: column {} will receive join key from outer",
                param_col_idx
            );
        }
    }

    PgMemoryContexts::For(state.tmp_ctx).switch_to(|_| {
        let relation = pg_sys::relation_open(foreigntableid, pg_sys::AccessShareLock as _);

        let mut pushdown_analysis = WhereClausePushdown::analyze_scan_clauses(
            scan_clauses,
            &state.table_type,
            relation as _,
        );
        log!(
            "WHERE clause pushdown analysis result: {:?}",
            pushdown_analysis
        );

        pushdown_analysis.set_limit_offset(extract_limit_offset_info(root));

        log!(
            "Extracted LIMIT/OFFSET info: {:?}",
            pushdown_analysis.limit_offset
        );

        if pushdown_analysis.has_optimizations() {
            log!(
                "Pushdown optimizations enabled: WHERE conditions={:?}, LIMIT/OFFSET={:?}",
                pushdown_analysis.pushable_conditions,
                pushdown_analysis.limit_offset
            );
        } else {
            log!("No pushdown optimizations possible");
        }

        state.set_pushdown_analysis(pushdown_analysis);

        pg_sys::relation_close(relation, pg_sys::AccessShareLock as _);
    });

    let fdw_private = serialize_ptr_to_list((*baserel).fdw_private);
    pgrx::pg_sys::make_foreignscan(
        tlist,
        pg_sys::extract_actual_clauses(scan_clauses, false),
        (*baserel).relid,
        fdw_exprs,
        fdw_private as _,
        ptr::null_mut(),
        ptr::null_mut(),
        outer_plan,
    )
}

#[pg_guard]
extern "C-unwind" fn begin_foreign_scan(
    node: *mut pgrx::pg_sys::ForeignScanState,
    _eflags: ::std::os::raw::c_int,
) {
    log!("---> begin_foreign_scan");
    unsafe {
        let scan_state = (*node).ss;
        let plan: *mut pg_sys::ForeignScan = scan_state.ps.plan as *mut pg_sys::ForeignScan;
        let scanrelid = (*plan).scan.scanrelid;

        if scanrelid == 0 {
            begin_foreign_join_scan(node, plan);
            return;
        }

        let relation = (*node).ss.ss_currentRelation;
        let relid = (*relation).rd_id;
        let state_ptr = deserialize_ptr_from_list((*plan).fdw_private as _);
        let state = state_from_ptr(state_ptr);
        PgMemoryContexts::For(state.tmp_ctx).switch_to(|_| {
            let options = get_foreign_table_options(relid);
            log!("Foreign table options: {:?}", options);
            state.update_from_options(options);

            if state.redis_connection.is_none() {
                if let Err(e) = state.init_redis_connection_from_options() {
                    pgrx::error!("Failed to connect to Redis: {}", e);
                }
            }

            state.set_table_type();
        });

        let relation = (*node).ss.ss_currentRelation;
        let tupdesc = (*relation).rd_att;
        state.ttl_column_index = detect_ttl_column(tupdesc);
        let mut col_names = extract_column_names(tupdesc);
        if let Some(ttl_idx) = state.ttl_column_index {
            if ttl_idx < col_names.len() {
                col_names.remove(ttl_idx);
            }
        }
        state.column_names = col_names;

        validate_column_count(
            &state.table_type,
            state.column_names.len(),
            state.is_multi_key,
        );

        let pushdown_idx =
            compute_pushdown_column_index(state.ttl_column_index, state.is_multi_key);

        // Compute score column index for ZSet (next active non-dropped, non-TTL column)
        let score_column_index = if matches!(state.table_type, RedisTableType::ZSet(_)) {
            let mut score_idx = pushdown_idx + 1;
            let natts = (*tupdesc).natts as usize;
            while score_idx < natts {
                let attr = tuple_desc_attr(tupdesc, score_idx);
                if (*attr).attisdropped || Some(score_idx) == state.ttl_column_index {
                    score_idx += 1;
                    continue;
                }
                break;
            }
            Some(score_idx)
        } else {
            None
        };

        state
            .table_type
            .configure(&state.column_names, pushdown_idx, score_column_index);

        if state.ttl_column_index.is_some() && !state.is_multi_key {
            let key = state.table_key_prefix.clone();
            state.read_ttl(&key);
        }

        if state.is_parameterized && !(*plan).fdw_exprs.is_null() {
            let expr_list = (*plan).fdw_exprs;
            if pg_sys::list_length(expr_list) > 0 {
                let first_expr = pg_sys::list_nth(expr_list, 0) as *mut pg_sys::Expr;
                let plan_state = &mut (*node).ss.ps as *mut pg_sys::PlanState;
                state.param_expr_state = pg_sys::ExecInitExpr(first_expr, plan_state);
                state.param_plan_state = plan_state;
                log!("Parameterized scan: ExprState initialized for join key evaluation");
            }
        }

        log!("Connected to Redis");
        (*node).fdw_state = state_ptr;
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn iterate_foreign_scan(
    node: *mut pgrx::pg_sys::ForeignScanState,
) -> *mut pgrx::pg_sys::TupleTableSlot {
    log!("---> iterate_foreign_scan");
    pgrx::check_for_interrupts!();
    let state = state_from_ptr((*node).fdw_state);
    let slot = (*node).ss.ss_ScanTupleSlot;
    let tupdesc = (*slot).tts_tupleDescriptor;

    exec_clear_tuple(slot);

    // Join pushdown mode
    if state.is_join_scan {
        if let Some(ref mut join_state) = state.join_state {
            if !state.join_executed {
                execute_foreign_join(join_state);
                state.join_executed = true;
            }
            if join_state.current_row < join_state.result_indices.len() {
                let natts = (*tupdesc).natts as usize;
                let outer_cols = crate::join::foreign_join::expected_columns_for_type(
                    &join_state.outer_table_type,
                );

                let (outer_row, inner_row) =
                    match &join_state.result_indices[join_state.current_row] {
                        crate::join::types::JoinResultRow::Matched {
                            outer_idx,
                            inner_idx,
                        } => (
                            &join_state.outer_data[*outer_idx],
                            Some(&join_state.inner_data[*inner_idx]),
                        ),
                        crate::join::types::JoinResultRow::OuterOnly { outer_idx } => {
                            (&join_state.outer_data[*outer_idx], None)
                        }
                    };

                let limit = std::cmp::min(natts, outer_cols);
                for col_idx in 0..limit {
                    if let Some(v) = outer_row.get(col_idx) {
                        write_datum_to_slot(slot, tupdesc, col_idx, v);
                    } else {
                        (*slot).tts_isnull.add(col_idx).write(true);
                    }
                }

                for col_idx in limit..natts {
                    let inner_col = col_idx - outer_cols;
                    if let Some(inner) = inner_row {
                        if let Some(v) = inner.get(inner_col) {
                            write_datum_to_slot(slot, tupdesc, col_idx, v);
                            continue;
                        }
                    }
                    (*slot).tts_isnull.add(col_idx).write(true);
                }

                join_state.current_row += 1;
                pgrx::pg_sys::ExecStoreVirtualTuple(slot);
                return slot;
            }
        }
        return slot;
    }

    // Parameterized scan: point-lookup
    if state.is_parameterized && !state.param_expr_state.is_null() {
        if state.row_count > 0 {
            return slot;
        }

        let econtext = (*node).ss.ps.ps_ExprContext;
        let mut is_null: bool = false;
        let datum = pg_sys::ExecEvalExpr(state.param_expr_state, econtext, &mut is_null);
        if is_null {
            return slot;
        }

        let param_value = datum_to_text_string(datum, state.param_type_oid);

        if state.parameterized_lookup(&param_value) {
            if state.ttl_column_index.is_some() {
                let ttl_key = if state.is_multi_key {
                    param_value.as_str()
                } else {
                    state.table_key_prefix.as_str()
                };
                let ttl_key_owned = ttl_key.to_string();
                state.cached_ttl = Some(state.read_ttl(&ttl_key_owned));
            }
            let natts_param = (*tupdesc).natts as usize;
            if let Some(row_data) = state.get_row(0) {
                let mut data_idx = 0;
                for col_idx in 0..natts_param {
                    if state.ttl_column_index == Some(col_idx) {
                        let val = state.cached_ttl.unwrap_or(-2);
                        write_datum_to_slot(slot, tupdesc, col_idx, &val.to_string());
                    } else if data_idx < row_data.len() {
                        write_datum_to_slot(slot, tupdesc, col_idx, row_data[data_idx].as_ref());
                        data_idx += 1;
                    }
                }
            }
            state.row_count = 1;
            pg_sys::ExecStoreVirtualTuple(slot);
            return slot;
        }
        return slot;
    }

    // Streaming iteration
    if state.is_read_end() {
        if state.scan_complete {
            return slot;
        }
        if !state.fetch_next_batch() {
            return slot;
        }
        state.row_count = 0;
    }

    if state.data_len() == 0 {
        return slot;
    }

    let natts = (*tupdesc).natts as usize;

    if state.is_multi_key {
        let cols_per_row = state.multi_key_columns_per_row();
        let row_offset = state.row_count as usize * cols_per_row;
        let dataset = state.table_type.get_dataset_ref();
        if let Some(flat_data) = dataset.as_filtered() {
            if row_offset + cols_per_row <= flat_data.len() {
                let ttl_value = if state.ttl_column_index.is_some() {
                    state
                        .multi_key_ttl_cache
                        .get(&flat_data[row_offset])
                        .copied()
                } else {
                    None
                };

                let mut data_idx = 0;
                for col_idx in 0..natts {
                    if state.ttl_column_index == Some(col_idx) {
                        let ttl_str = ttl_value.unwrap_or(-2).to_string();
                        write_datum_to_slot(slot, tupdesc, col_idx, &ttl_str);
                    } else if data_idx < cols_per_row {
                        write_datum_to_slot(
                            slot,
                            tupdesc,
                            col_idx,
                            &flat_data[row_offset + data_idx],
                        );
                        data_idx += 1;
                    }
                }
            } else {
                return slot;
            }
        } else {
            return slot;
        }
    } else {
        let ttl_value = if state.ttl_column_index.is_some() {
            state.cached_ttl
        } else {
            None
        };
        if let Some(row_data) = state.get_row(state.row_count as usize) {
            let mut data_idx = 0;
            for col_idx in 0..natts {
                if state.ttl_column_index == Some(col_idx) {
                    let val = ttl_value.unwrap_or(-2);
                    write_datum_to_slot(slot, tupdesc, col_idx, &val.to_string());
                } else if data_idx < row_data.len() {
                    write_datum_to_slot(slot, tupdesc, col_idx, row_data[data_idx].as_ref());
                    data_idx += 1;
                }
            }
        } else {
            error!(
                "Failed to get row data at index: {} (data_len={})",
                state.row_count,
                state.data_len()
            );
        }
    }

    state.row_count += 1;
    pgrx::pg_sys::ExecStoreVirtualTuple(slot);
    slot
}

#[pg_guard]
extern "C-unwind" fn end_foreign_scan(node: *mut pgrx::pg_sys::ForeignScanState) {
    log!("---> end_foreign_scan");
    unsafe {
        let fdw_state = (*node).fdw_state as *mut RedisFdwState;
        if fdw_state.is_null() {
            return;
        }
        let state = &mut *fdw_state;
        let ctx = state.tmp_ctx;
        if !ctx.is_null() {
            delete_wrappers_memctx(ctx);
        }
    }
}

#[pg_guard]
extern "C-unwind" fn re_scan_foreign_scan(node: *mut pgrx::pg_sys::ForeignScanState) {
    log!("---> re_scan_foreign_scan");
    unsafe {
        let fdw_state = (*node).fdw_state as *mut RedisFdwState;
        if fdw_state.is_null() {
            return;
        }
        let state = &mut *fdw_state;

        if state.is_join_scan {
            if let Some(ref mut join_state) = state.join_state {
                join_state.current_row = 0;
            }
        } else {
            state.row_count = 0;
            state.scan_cursor = 0;
            state.scan_complete = false;
            state.cached_ttl = None;
            state.multi_key_ttl_cache.clear();
            state.table_type.clear_data();
        }
    }
}

#[pg_guard]
extern "C-unwind" fn recheck_foreign_scan(
    _node: *mut pgrx::pg_sys::ForeignScanState,
    _slot: *mut pgrx::pg_sys::TupleTableSlot,
) -> bool {
    log!("---> recheck_foreign_scan");
    true
}

#[pg_guard]
extern "C-unwind" fn shutdown_foreign_scan(node: *mut pgrx::pg_sys::ForeignScanState) {
    log!("---> shutdown_foreign_scan");
    unsafe {
        let fdw_state = (*node).fdw_state as *mut RedisFdwState;
        if fdw_state.is_null() {
            return;
        }
        let state = &mut *fdw_state;
        state.redis_connection = None;
        if let Some(ref mut join_state) = state.join_state {
            join_state.connection = None;
            join_state.outer_data = Vec::new();
            join_state.inner_data = Vec::new();
            join_state.result_indices = Vec::new();
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn add_foreign_update_targets(
    root: *mut pgrx::pg_sys::PlannerInfo,
    rtindex: pgrx::pg_sys::Index,
    _target_rte: *mut pgrx::pg_sys::RangeTblEntry,
    target_relation: pgrx::pg_sys::Relation,
) {
    log!("---> add_foreign_update_targets");
    let tupdesc = relation_get_descr(target_relation);
    let ttl_idx = detect_ttl_column(tupdesc);
    let natts = (*tupdesc).natts as usize;

    let relid = (*target_relation).rd_id;
    let opts = get_foreign_table_options(relid);
    let table_type = opts.get("table_type").map(|s| s.as_str()).unwrap_or("");

    // Count active (non-dropped, non-TTL) data columns
    let mut num_data_cols = 0usize;
    for i in 0..natts {
        let a = tuple_desc_attr(tupdesc, i);
        if (*a).attisdropped {
            continue;
        }
        if Some(i) == ttl_idx {
            continue;
        }
        num_data_cols += 1;
    }

    let identity_attno = if table_type == "list" && num_data_cols >= 2 {
        // For 2-column list (index, value): LREM needs the value column,
        // which is the second active non-TTL column
        let mut count = 0usize;
        let mut target = 0usize;
        for i in 0..natts {
            let a = tuple_desc_attr(tupdesc, i);
            if (*a).attisdropped {
                continue;
            }
            if Some(i) == ttl_idx {
                continue;
            }
            count += 1;
            if count == 2 {
                target = i;
                break;
            }
        }
        target
    } else {
        // Default: first active non-TTL column
        let mut target = 0usize;
        for i in 0..natts {
            let a = tuple_desc_attr(tupdesc, i);
            if (*a).attisdropped {
                continue;
            }
            if Some(i) == ttl_idx {
                continue;
            }
            target = i;
            break;
        }
        target
    };

    let attr = *tuple_desc_attr(tupdesc, identity_attno);
    let varattno = (identity_attno as i16) + 1;

    let var = pg_sys::makeVar(
        rtindex as _,
        varattno,
        attr.atttypid,
        attr.atttypmod,
        pg_sys::InvalidOid,
        0,
    );

    pg_sys::add_row_identity_var(root, var, rtindex, REDISMODY.as_ptr() as _);
}

#[pg_guard]
unsafe extern "C-unwind" fn plan_foreign_modify(
    root: *mut PlannerInfo,
    _plan: *mut ModifyTable,
    result_relation: Index,
    _subplan_index: ::core::ffi::c_int,
) -> *mut pgrx::pg_sys::List {
    log!("---> plan_foreign_modify");
    let rte = pg_sys::planner_rt_fetch(result_relation, root);
    let rel = PgRelation::with_lock((*rte).relid, pg_sys::NoLock as _);
    let ftable_id = rel.oid();
    let ctx_name = format!("Wrappers_modify_{}", ftable_id.to_u32());
    let ctx = create_wrappers_memctx(&ctx_name);
    let mut state: RedisFdwState = RedisFdwState::new(ctx);
    PgMemoryContexts::For(state.tmp_ctx).switch_to(|_| {
        let opts = get_foreign_table_options(ftable_id);
        log!("Foreign table options for modify: {:?}", opts);
        state.update_from_options(opts);

        if let Err(e) = state.init_redis_connection_from_options() {
            pgrx::error!("Failed to connect to Redis: {}", e);
        }

        state.set_table_type();
    });
    let state_ptr = PgMemoryContexts::For(ctx).leak_and_drop_on_delete(state);
    serialize_ptr_to_list(state_ptr as *mut std::os::raw::c_void)
}

#[pg_guard]
unsafe extern "C-unwind" fn begin_foreign_modify(
    mtstate: *mut pgrx::pg_sys::ModifyTableState,
    rinfo: *mut pgrx::pg_sys::ResultRelInfo,
    fdw_private: *mut pgrx::pg_sys::List,
    _subplan_index: ::std::os::raw::c_int,
    _eflags: ::std::os::raw::c_int,
) {
    log!("---> begin_foreign_modify");
    let state_ptr = deserialize_ptr_from_list(fdw_private as _);
    let state = state_from_ptr(state_ptr);
    let subplan = (*outer_plan_state(&mut (*mtstate).ps)).plan;
    state.key_attno =
        pg_sys::ExecFindJunkAttributeInTlist((*subplan).targetlist, REDISMODY.as_ptr() as _);
    log!("Key attribute number: {}", state.key_attno);

    let relation = (*rinfo).ri_RelationDesc;
    let tupdesc = (*relation).rd_att;
    state.ttl_column_index = detect_ttl_column(tupdesc);
    let mut col_names = extract_column_names(tupdesc);
    if let Some(ttl_idx) = state.ttl_column_index {
        if ttl_idx < col_names.len() {
            col_names.remove(ttl_idx);
        }
    }
    state.column_names = col_names;

    validate_column_count(
        &state.table_type,
        state.column_names.len(),
        state.is_multi_key,
    );

    let pushdown_idx = compute_pushdown_column_index(state.ttl_column_index, state.is_multi_key);
    let score_column_index = if matches!(state.table_type, RedisTableType::ZSet(_)) {
        let mut score_idx = pushdown_idx + 1;
        let natts = (*tupdesc).natts as usize;
        while score_idx < natts {
            let attr = tuple_desc_attr(tupdesc, score_idx);
            if (*attr).attisdropped || Some(score_idx) == state.ttl_column_index {
                score_idx += 1;
                continue;
            }
            break;
        }
        Some(score_idx)
    } else {
        None
    };
    state
        .table_type
        .configure(&state.column_names, pushdown_idx, score_column_index);

    (*rinfo).ri_FdwState = state_ptr;
}

#[inline]
pub(super) unsafe fn outer_plan_state(node: *mut pg_sys::PlanState) -> *mut pg_sys::PlanState {
    (*node).lefttree
}

#[pg_guard]
unsafe extern "C-unwind" fn exec_foreign_insert(
    _estate: *mut pgrx::pg_sys::EState,
    rinfo: *mut pgrx::pg_sys::ResultRelInfo,
    slot: *mut pgrx::pg_sys::TupleTableSlot,
    _plan_slot: *mut pgrx::pg_sys::TupleTableSlot,
) -> *mut pgrx::pg_sys::TupleTableSlot {
    log!("---> exec_foreign_insert");
    let state = state_from_ptr((*rinfo).ri_FdwState);
    let row: Row = tuple_table_slot_to_row(slot);

    let all_data: Vec<String> = row
        .cells
        .iter()
        .map(|cell| cell_to_string(cell.as_ref()))
        .collect();

    let (data, row_ttl) = if let Some(ttl_idx) = state.ttl_column_index {
        let ttl_val = all_data.get(ttl_idx).and_then(|s| {
            if s == "NULL" {
                None
            } else {
                s.parse::<i64>().ok()
            }
        });
        let mut data = all_data;
        if ttl_idx < data.len() {
            data.remove(ttl_idx);
        }
        (data, ttl_val)
    } else {
        (all_data, None)
    };

    if state.is_multi_key {
        if data.is_empty() {
            error!("Multi-key INSERT requires at least a key column");
        }
        let key = data[0].clone();
        validate_key_prefix(
            &key,
            extract_static_prefix(&state.table_key_prefix),
            &state.table_key_prefix,
            state.strict_key_prefix,
        );
        let row_data = &data[1..];
        let required_cols = state.multi_key_columns_per_row() - 1;
        if row_data.len() < required_cols {
            error!(
                "Multi-key INSERT requires {} data columns, got {}",
                required_cols,
                row_data.len()
            );
        }
        if let Err(e) = state.insert_data_to_key(&key, row_data) {
            error!("Failed to insert data to key '{}': {:?}", key, e);
        }
        state.apply_ttl(&key, row_ttl);
    } else {
        let data = transform_insert_data(&state.table_type, &state.column_names, data);
        if let Err(e) = state.insert_data(&data) {
            error!("Failed to insert data: {:?}", e);
        }
        let key = state.table_key_prefix.clone();
        state.apply_ttl(&key, row_ttl);
    }

    (*slot).tts_tableOid = pgrx::pg_sys::InvalidOid;
    slot
}

#[pg_guard]
unsafe extern "C-unwind" fn exec_foreign_update(
    _estate: *mut pgrx::pg_sys::EState,
    rinfo: *mut pgrx::pg_sys::ResultRelInfo,
    slot: *mut pgrx::pg_sys::TupleTableSlot,
    plan_slot: *mut pgrx::pg_sys::TupleTableSlot,
) -> *mut pgrx::pg_sys::TupleTableSlot {
    log!("---> exec_foreign_update");
    let state = state_from_ptr((*rinfo).ri_FdwState);

    let old_key = match extract_delete_key(state, plan_slot) {
        Ok(key) => key,
        Err(err_msg) => {
            error!("Failed to extract old key for update: {}", err_msg);
        }
    };

    let new_row: Row = tuple_table_slot_to_row(slot);
    let all_new_data: Vec<String> = new_row
        .cells
        .iter()
        .map(|cell| cell_to_string(cell.as_ref()))
        .collect();

    let (new_data, row_ttl) = if let Some(ttl_idx) = state.ttl_column_index {
        let ttl_val = all_new_data.get(ttl_idx).and_then(|s| {
            if s == "NULL" {
                None
            } else {
                s.parse::<i64>().ok()
            }
        });
        let mut data = all_new_data;
        if ttl_idx < data.len() {
            data.remove(ttl_idx);
        }
        (data, ttl_val)
    } else {
        (all_new_data, None)
    };

    log!("Update: old_key={:?}, new_data={:?}", old_key, new_data);

    if state.is_multi_key {
        if new_data.is_empty() {
            error!("Multi-key UPDATE requires at least a key column");
        }
        let key = new_data[0].clone();
        validate_key_prefix(
            &key,
            extract_static_prefix(&state.table_key_prefix),
            &state.table_key_prefix,
            state.strict_key_prefix,
        );
        let row_data = &new_data[1..];
        let required_cols = state.multi_key_columns_per_row() - 1;
        if row_data.len() < required_cols {
            error!(
                "Multi-key UPDATE requires {} data columns, got {}",
                required_cols,
                row_data.len()
            );
        }
        if let Err(e) = state.update_data_to_key(&key, std::slice::from_ref(&old_key), row_data) {
            error!("Failed to update data for key '{}': {:?}", key, e);
        }
        state.apply_ttl(&key, row_ttl);
    } else {
        if let Err(e) = state.update_data(std::slice::from_ref(&old_key), &new_data) {
            error!("Failed to update data: {:?}", e);
        }
        let key = state.table_key_prefix.clone();
        state.apply_ttl(&key, row_ttl);
    }

    (*slot).tts_tableOid = pgrx::pg_sys::InvalidOid;
    slot
}

#[pg_guard]
unsafe extern "C-unwind" fn exec_foreign_delete(
    _estate: *mut pgrx::pg_sys::EState,
    rinfo: *mut pgrx::pg_sys::ResultRelInfo,
    slot: *mut pgrx::pg_sys::TupleTableSlot,
    plan_slot: *mut pgrx::pg_sys::TupleTableSlot,
) -> *mut pgrx::pg_sys::TupleTableSlot {
    log!("---> exec_foreign_delete");

    let state = state_from_ptr((*rinfo).ri_FdwState);

    match extract_delete_key(state, plan_slot) {
        Ok(key) => {
            log!("Attempting to delete key: '{}'", key);

            if state.is_multi_key {
                if let Err(e) = state.delete_key(&key) {
                    error!("Failed to delete Redis key '{}': {:?}", key, e);
                }
            } else if let Err(e) = state.delete_data(std::slice::from_ref(&key)) {
                error!("Failed to delete key '{}': {:?}", key, e);
            }
            log!("Successfully deleted key: '{}'", key);
        }
        Err(err_msg) => {
            error!("Failed to extract delete key: {}", err_msg);
        }
    }

    (*slot).tts_tableOid = pgrx::pg_sys::InvalidOid;

    slot
}

#[pg_guard]
unsafe extern "C-unwind" fn end_foreign_modify(
    _estate: *mut pgrx::pg_sys::EState,
    rinfo: *mut pgrx::pg_sys::ResultRelInfo,
) {
    log!("---> end_foreign_modify");
    unsafe {
        let fdw_state = (*rinfo).ri_FdwState as *mut RedisFdwState;
        if fdw_state.is_null() {
            return;
        }

        let state = &*fdw_state;
        let ctx = state.tmp_ctx;
        if !ctx.is_null() {
            delete_wrappers_memctx(ctx);
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn begin_foreign_insert(
    _mtstate: *mut pgrx::pg_sys::ModifyTableState,
    rinfo: *mut pgrx::pg_sys::ResultRelInfo,
) {
    log!("---> begin_foreign_insert");
    let relation = (*rinfo).ri_RelationDesc;
    let ftable_id = (*relation).rd_id;
    let ctx_name = format!("Wrappers_insert_{}", ftable_id.to_u32());
    let ctx = create_wrappers_memctx(&ctx_name);
    let mut state = RedisFdwState::new(ctx);
    PgMemoryContexts::For(state.tmp_ctx).switch_to(|_| {
        let opts = get_foreign_table_options(ftable_id);
        log!("Foreign table options for insert: {:?}", opts);
        state.update_from_options(opts);

        if let Err(e) = state.init_redis_connection_from_options() {
            pgrx::error!("Failed to connect to Redis: {}", e);
        }

        state.set_table_type();
    });

    let tupdesc = (*relation).rd_att;
    state.ttl_column_index = detect_ttl_column(tupdesc);
    let mut col_names = extract_column_names(tupdesc);
    if let Some(ttl_idx) = state.ttl_column_index {
        if ttl_idx < col_names.len() {
            col_names.remove(ttl_idx);
        }
    }
    state.column_names = col_names;

    let pushdown_idx = compute_pushdown_column_index(state.ttl_column_index, state.is_multi_key);
    let score_column_index = if matches!(state.table_type, RedisTableType::ZSet(_)) {
        let mut score_idx = pushdown_idx + 1;
        let natts = (*tupdesc).natts as usize;
        while score_idx < natts {
            let attr = tuple_desc_attr(tupdesc, score_idx);
            if (*attr).attisdropped || Some(score_idx) == state.ttl_column_index {
                score_idx += 1;
                continue;
            }
            break;
        }
        Some(score_idx)
    } else {
        None
    };
    state
        .table_type
        .configure(&state.column_names, pushdown_idx, score_column_index);

    let state_ptr = PgMemoryContexts::For(ctx).leak_and_drop_on_delete(state);
    (*rinfo).ri_FdwState = state_ptr as *mut std::os::raw::c_void;
}

#[pg_guard]
unsafe extern "C-unwind" fn end_foreign_insert(
    _estate: *mut pgrx::pg_sys::EState,
    rinfo: *mut pgrx::pg_sys::ResultRelInfo,
) {
    log!("---> end_foreign_insert");
    let fdw_state = (*rinfo).ri_FdwState as *mut RedisFdwState;
    if fdw_state.is_null() {
        return;
    }

    let state = &*fdw_state;
    let ctx = state.tmp_ctx;
    if !ctx.is_null() {
        delete_wrappers_memctx(ctx);
    }
}

#[pg_guard]
extern "C-unwind" fn is_foreign_rel_updatable(
    rel: pgrx::pg_sys::Relation,
) -> ::std::os::raw::c_int {
    log!("---> is_foreign_rel_updatable");
    unsafe {
        let relid = (*rel).rd_id;
        let options = get_foreign_table_options(relid);
        let table_type = options.get("table_type").map(|s| s.as_str()).unwrap_or("");

        match table_type.to_lowercase().as_str() {
            "stream" => (1 << 3) | (1 << 4),
            _ => (1 << 2) | (1 << 3) | (1 << 4),
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn get_foreign_modify_batch_size(
    rinfo: *mut pg_sys::ResultRelInfo,
) -> ::core::ffi::c_int {
    log!("---> get_foreign_modify_batch_size");
    let fdw_state = (*rinfo).ri_FdwState as *mut RedisFdwState;
    if fdw_state.is_null() {
        return 1;
    }
    let state = &*fdw_state;
    state.batch_size as ::core::ffi::c_int
}

#[pg_guard]
unsafe extern "C-unwind" fn exec_foreign_batch_insert(
    _estate: *mut pg_sys::EState,
    rinfo: *mut pg_sys::ResultRelInfo,
    slots: *mut *mut pg_sys::TupleTableSlot,
    _plan_slots: *mut *mut pg_sys::TupleTableSlot,
    num_slots: *mut ::core::ffi::c_int,
) -> *mut *mut pg_sys::TupleTableSlot {
    log!("---> exec_foreign_batch_insert");
    let state = state_from_ptr((*rinfo).ri_FdwState);
    let count = *num_slots as usize;

    let mut rows: Vec<(Vec<String>, Option<i64>)> = Vec::with_capacity(count);
    for i in 0..count {
        let slot = *slots.add(i);
        let row: Row = tuple_table_slot_to_row(slot);
        let all_data: Vec<String> = row
            .cells
            .iter()
            .map(|cell| cell_to_string(cell.as_ref()))
            .collect();

        let (data, row_ttl) = if let Some(ttl_idx) = state.ttl_column_index {
            let ttl_val = all_data.get(ttl_idx).and_then(|s| {
                if s == "NULL" {
                    None
                } else {
                    s.parse::<i64>().ok()
                }
            });
            let mut data = all_data;
            if ttl_idx < data.len() {
                data.remove(ttl_idx);
            }
            (data, ttl_val)
        } else {
            (all_data, None)
        };
        let data = transform_insert_data(&state.table_type, &state.column_names, data);
        rows.push((data, row_ttl));
    }

    if let Err(e) = state.batch_insert_data(&rows) {
        error!("{}", e);
    }

    for i in 0..count {
        let slot = *slots.add(i);
        (*slot).tts_tableOid = pg_sys::InvalidOid;
    }

    slots
}
