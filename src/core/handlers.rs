use crate::{
    core::state_manager::RedisFdwState,
    join::{
        foreign_join::execute_foreign_join,
        types::{RedisJoinState, RedisJoinType},
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
use std::ffi::CString;
use std::ptr;

#[inline]
unsafe fn state_from_ptr<'a>(ptr: *mut std::os::raw::c_void) -> &'a mut RedisFdwState {
    if ptr.is_null() {
        pgrx::error!("Redis FDW state pointer is null");
    }
    &mut *(ptr as *mut RedisFdwState)
}

unsafe fn detect_ttl_column(tupdesc: pg_sys::TupleDesc) -> Option<usize> {
    let natts = (*tupdesc).natts as usize;
    for i in 0..natts {
        let attr = tuple_desc_attr(tupdesc, i);
        if (*attr).attisdropped {
            continue;
        }
        let name = pgrx::name_data_to_str(&(*attr).attname);
        if name.eq_ignore_ascii_case("ttl") {
            return Some(i);
        }
    }
    None
}

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

        // Initialize the state with table options for pushdown analysis
        let options = get_foreign_table_options(foreigntableid);
        log!("Foreign table options: {:?}", options);
        state.update_from_options(options);

        // Set table type so pushdown analysis knows what optimizations are possible
        if let Some(table_type_str) = state.opts.get("table_type") {
            state.table_type = RedisTableType::from_str(table_type_str);
        }

        // Connect to Redis for real statistics gathering
        if let Err(e) = state.init_redis_connection_from_options() {
            log!(
                "Could not connect to Redis for cost estimation, using defaults: {}",
                e
            );
        }

        // Calculate cost estimate using actual Redis statistics
        let cost_estimate = state.estimate_costs();
        log!(
            "Cost estimation: rows={}, startup_cost={}, total_cost={}, width={}",
            cost_estimate.rows,
            cost_estimate.startup_cost,
            cost_estimate.total_cost,
            cost_estimate.width
        );

        // Release planning-phase connection back to pool immediately, begin_foreign_scan will re-acquire from pool (fast path: read-lock only).
        state.redis_connection = None;

        // Store the estimate for use in get_foreign_paths
        let estimated_rows = cost_estimate.rows;
        state.cost_estimate = Some(cost_estimate);

        // Allocate state in PG memory context — Drop fires on context destruction (longjmp-safe)
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
        // Retrieve cost estimate from state (calculated in get_foreign_rel_size)
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
                // Fallback to quick estimate if no cached estimate
                log!("No cached estimate, using fallback costs");
                (10.0, (*baserel).rows * 0.1 + 10.0)
            }
        } else {
            // Default fallback costs
            log!("State not available, using default costs");
            (10.0, 100.0)
        };

        let path = pgrx::pg_sys::create_foreignscan_path(
            _root,
            baserel,
            ptr::null_mut(),
            (*baserel).rows,
            #[cfg(feature = "pg18")]
            0, // disabled_nodes
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

    // Join relation: scanrelid=0, build join plan
    if (*baserel).reloptkind == pg_sys::RelOptKind::RELOPT_JOINREL {
        return plan_foreign_join(root, baserel, best_path, tlist, outer_plan);
    }

    // Base relation: normal scan plan
    let state = state_from_ptr((*baserel).fdw_private);

    PgMemoryContexts::For(state.tmp_ctx).switch_to(|_| {
        let relation = pg_sys::relation_open(foreigntableid, pg_sys::AccessShareLock as _);

        // Analyze WHERE clauses for pushdown opportunities
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

        // Store the analysis in the state
        state.set_pushdown_analysis(pushdown_analysis);

        pg_sys::relation_close(relation, pg_sys::AccessShareLock as _);
    });

    // Serialize state pointer to a proper PG List for safe plan copying
    let fdw_private = serialize_ptr_to_list((*baserel).fdw_private);
    pgrx::pg_sys::make_foreignscan(
        tlist,
        pg_sys::extract_actual_clauses(scan_clauses, false),
        (*baserel).relid,
        ptr::null_mut(),
        fdw_private as _,
        ptr::null_mut(),
        ptr::null_mut(),
        outer_plan,
    )
}

unsafe fn plan_foreign_join(
    _root: *mut pgrx::pg_sys::PlannerInfo,
    joinrel: *mut pgrx::pg_sys::RelOptInfo,
    best_path: *mut pgrx::pg_sys::ForeignPath,
    tlist: *mut pgrx::pg_sys::List,
    outer_plan: *mut pgrx::pg_sys::Plan,
) -> *mut pgrx::pg_sys::ForeignScan {
    log!("---> plan_foreign_join");

    // Extract outer/inner state from the ForeignPath's fdw_private
    let path_private = (*best_path).fdw_private;
    let outer_state_ptr = deserialize_nth_ptr_from_list(path_private, 0) as *mut RedisFdwState;
    let inner_state_ptr = deserialize_nth_ptr_from_list(path_private, 1) as *mut RedisFdwState;

    if outer_state_ptr.is_null() || inner_state_ptr.is_null() {
        pgrx::error!("Join plan: missing outer/inner state in fdw_private");
    }

    let outer_state = &*outer_state_ptr;
    let inner_state = &*inner_state_ptr;

    // Read the join type from the third list element
    let jointype_val = deserialize_nth_ptr_from_list(path_private, 2) as i64;
    let join_type = if jointype_val == pgrx::pg_sys::JoinType::JOIN_LEFT as i64 {
        RedisJoinType::Left
    } else {
        RedisJoinType::Inner
    };

    // Read join columns from 4th and 5th list elements
    let join_col_outer = deserialize_nth_ptr_from_list(path_private, 3) as usize;
    let join_col_inner = deserialize_nth_ptr_from_list(path_private, 4) as usize;

    // Build the RedisJoinState with table info from both sides
    let join_state = RedisJoinState::new(
        outer_state.table_type.clone(),
        inner_state.table_type.clone(),
        outer_state.table_key_prefix.clone(),
        inner_state.table_key_prefix.clone(),
        join_type,
        join_col_outer,
        join_col_inner,
    );

    // Create a new RedisFdwState for the join scan
    let ctx_name = "Wrappers_join_scan";
    let ctx = create_wrappers_memctx(ctx_name);
    let mut state = RedisFdwState::new(ctx);

    // Copy connection info from outer (same server guaranteed by get_foreign_join_paths)
    state.host_port = outer_state.host_port.clone();
    state.database = outer_state.database;
    state.opts = outer_state.opts.clone();
    state.is_join_scan = true;
    state.join_state = Some(join_state);

    let state_ptr = PgMemoryContexts::For(ctx).leak_and_drop_on_delete(state);
    (*joinrel).fdw_private = state_ptr as *mut std::os::raw::c_void;

    // Build fdw_scan_tlist from the joinrel's reltarget for proper output mapping
    let reltarget = (*joinrel).reltarget;
    let fdw_scan_tlist = if !reltarget.is_null() && !(*reltarget).exprs.is_null() {
        pg_sys::add_to_flat_tlist(ptr::null_mut(), (*reltarget).exprs)
    } else {
        ptr::null_mut()
    };

    let fdw_private = serialize_ptr_to_list(state_ptr as *mut std::os::raw::c_void);
    pgrx::pg_sys::make_foreignscan(
        tlist,
        ptr::null_mut(), // no qual for join scan
        0,               // scanrelid=0 for join
        ptr::null_mut(),
        fdw_private as _,
        fdw_scan_tlist,
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

        // Join scan: scanrelid == 0 means this is a pushed-down join
        if scanrelid == 0 {
            begin_foreign_join_scan(node, plan);
            return;
        }

        // Normal base relation scan
        let relation = (*node).ss.ss_currentRelation;
        let relid = (*relation).rd_id;
        let state_ptr = deserialize_ptr_from_list((*plan).fdw_private as _);
        let state = state_from_ptr(state_ptr);
        PgMemoryContexts::For(state.tmp_ctx).switch_to(|_| {
            let options = get_foreign_table_options(relid);
            log!("Foreign table options: {:?}", options);
            state.update_from_options(options);

            // Acquire connection from pool (fast path: read-lock on existing pool)
            if state.redis_connection.is_none() {
                if let Err(e) = state.init_redis_connection_from_options() {
                    pgrx::error!("Failed to connect to Redis: {}", e);
                }
            }

            state.set_table_type();
        });

        // Detect TTL column
        let relation = (*node).ss.ss_currentRelation;
        let tupdesc = (*relation).rd_att;
        state.ttl_column_index = detect_ttl_column(tupdesc);

        // Pre-fetch TTL for single-key mode to avoid per-row Redis calls during iteration
        if state.ttl_column_index.is_some() && !state.is_multi_key {
            let key = state.table_key_prefix.clone();
            state.read_ttl(&key);
        }

        log!("Connected to Redis");
        (*node).fdw_state = state_ptr;
    }
}

unsafe fn begin_foreign_join_scan(
    node: *mut pgrx::pg_sys::ForeignScanState,
    plan: *mut pg_sys::ForeignScan,
) {
    log!("---> begin_foreign_join_scan");
    let state_ptr = deserialize_ptr_from_list((*plan).fdw_private as _);
    let state = state_from_ptr(state_ptr);

    // Connect to Redis for the join execution
    if state.redis_connection.is_none() {
        if let Err(e) = state.init_redis_connection_from_options() {
            pgrx::error!("Failed to connect to Redis for join scan: {}", e);
        }
    }

    // Transfer connection to the join_state so execute_foreign_join can use it
    if let Some(ref mut join_state) = state.join_state {
        join_state.connection = state.redis_connection.take();
    }

    (*node).fdw_state = state_ptr;
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

    // Join pushdown mode: execute join on first call, then iterate results
    if state.is_join_scan {
        if let Some(ref mut join_state) = state.join_state {
            if !state.join_executed {
                execute_foreign_join(join_state);
                state.join_executed = true;
            }
            if join_state.current_row < join_state.result_data.len() {
                let row = &join_state.result_data[join_state.current_row];
                let natts = (*tupdesc).natts as usize;
                for (col_idx, value) in row.iter().enumerate().take(natts) {
                    write_datum_to_slot(slot, tupdesc, col_idx, value);
                }
                join_state.current_row += 1;
                pgrx::pg_sys::ExecStoreVirtualTuple(slot);
                return slot;
            }
        }
        return slot;
    }

    // Streaming iteration: if current batch is exhausted, fetch more
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
        // Destroy the memory context — this triggers the registered Drop callback
        // which properly frees the RedisFdwState and all its owned Rust resources
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
            // Reset iterator to re-read materialized results without re-fetching from Redis
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
    let attr = *tuple_desc_attr(relation_get_descr(target_relation), 0);

    let var = pg_sys::makeVar(
        rtindex as _,
        1, //attr.attnum,
        attr.atttypid,
        attr.atttypmod,
        pg_sys::InvalidOid, //attr.attlen,
        0,
    );

    // register it as a row-identity column needed by this target rel
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

        // Initialize Redis connection and handle potential errors
        if let Err(e) = state.init_redis_connection_from_options() {
            pgrx::error!("Failed to connect to Redis: {}", e);
        }

        state.set_table_type();
    });
    // Allocate state in PG memory context — Drop fires on context destruction (longjmp-safe)
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

    // Detect TTL column in the target relation
    let relation = (*rinfo).ri_RelationDesc;
    let tupdesc = (*relation).rd_att;
    state.ttl_column_index = detect_ttl_column(tupdesc);

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

    // Extract and strip TTL column value
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
        // Multi-key mode: first column is the Redis key, remaining columns are data
        if data.is_empty() {
            error!("Multi-key INSERT requires at least a key column");
        }
        let key = data[0].clone();
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

    // Extract old key from plan_slot (junk attribute set by add_foreign_update_targets)
    let old_key = match extract_delete_key(state, plan_slot) {
        Ok(key) => key,
        Err(err_msg) => {
            error!("Failed to extract old key for update: {}", err_msg);
        }
    };

    // Extract new row from the slot
    let new_row: Row = tuple_table_slot_to_row(slot);
    let all_new_data: Vec<String> = new_row
        .cells
        .iter()
        .map(|cell| cell_to_string(cell.as_ref()))
        .collect();

    // Extract and strip TTL column value
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

    // Extract state and validate it's not null
    let state = state_from_ptr((*rinfo).ri_FdwState);

    // Extract the key attribute for deletion
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

    // Mark slot as having an invalid table OID since this is a delete operation
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
        // Destroy the memory context — this triggers the registered Drop callback
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
    // CmdType: CMD_UPDATE=2, CMD_INSERT=3, CMD_DELETE=4
    // Return bitmask: (1 << CMD_INSERT) | (1 << CMD_UPDATE) | (1 << CMD_DELETE)
    // = 8 | 4 | 16 = 28 for updatable types
    // = 8 | 16 = 24 for stream (no UPDATE)
    unsafe {
        let relid = (*rel).rd_id;
        let options = get_foreign_table_options(relid);
        let table_type = options.get("table_type").map(|s| s.as_str()).unwrap_or("");

        match table_type.to_lowercase().as_str() {
            "stream" => (1 << 3) | (1 << 4),     // INSERT | DELETE = 24
            _ => (1 << 2) | (1 << 3) | (1 << 4), // UPDATE | INSERT | DELETE = 28
        }
    }
}

unsafe fn extract_delete_key(
    state: &RedisFdwState,
    plan_slot: *mut pgrx::pg_sys::TupleTableSlot,
) -> Result<String, &'static str> {
    // Validate key attribute number
    if state.key_attno <= 0 {
        return Err("Invalid key attribute number");
    }

    // Extract the junk attribute (row identifier)
    let mut is_null = false;
    let datum = exec_get_junk_attribute(plan_slot, state.key_attno, &mut is_null);

    // Convert datum to string safely
    match String::from_datum(datum, is_null) {
        Some(key_string) => {
            if key_string.is_empty() {
                Err("Delete key is empty")
            } else {
                Ok(key_string)
            }
        }
        None => Err("Failed to convert datum to string"),
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn explain_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
    es: *mut pg_sys::ExplainState,
) {
    log!("---> explain_foreign_scan");
    let fdw_state = (*node).fdw_state as *mut RedisFdwState;
    if fdw_state.is_null() {
        return;
    }
    let state = &*fdw_state;

    if state.is_join_scan {
        let label_join = c"Redis Join";
        let join_desc = if let Some(ref js) = state.join_state {
            format!(
                "{}({}) x {}({})",
                js.outer_table_type.redis_type_name(),
                js.outer_key_prefix,
                js.inner_table_type.redis_type_name(),
                js.inner_key_prefix
            )
        } else {
            "FDW-to-FDW pushdown".to_string()
        };
        let join_cstr = CString::new(join_desc).unwrap_or_default();
        pg_sys::ExplainPropertyText(label_join.as_ptr(), join_cstr.as_ptr(), es);

        let server_cstr = CString::new(state.host_port.as_str()).unwrap_or_default();
        let label_server = c"Redis Server";
        pg_sys::ExplainPropertyText(label_server.as_ptr(), server_cstr.as_ptr(), es);
        return;
    }

    let server_cstr = CString::new(state.host_port.as_str()).unwrap_or_default();
    let key_cstr = CString::new(state.table_key_prefix.as_str()).unwrap_or_default();
    let type_name = state.table_type.redis_type_name();
    let type_cstr = CString::new(type_name).unwrap_or_default();
    let multi_key_cstr =
        CString::new(if state.is_multi_key { "true" } else { "false" }).unwrap_or_default();

    let label_server = c"Redis Server";
    let label_key = c"Redis Key";
    let label_type = c"Table Type";
    let label_multi = c"Multi-Key Mode";
    let label_pushdown = c"Pushdown";
    let label_batch = c"Batch Size";

    pg_sys::ExplainPropertyText(label_server.as_ptr(), server_cstr.as_ptr(), es);
    pg_sys::ExplainPropertyText(label_key.as_ptr(), key_cstr.as_ptr(), es);
    pg_sys::ExplainPropertyText(label_type.as_ptr(), type_cstr.as_ptr(), es);
    pg_sys::ExplainPropertyText(label_multi.as_ptr(), multi_key_cstr.as_ptr(), es);

    let pushdown_desc = if let Some(ref analysis) = state.pushdown_analysis {
        if analysis.has_optimizations() {
            analysis
                .pushable_conditions
                .iter()
                .map(|c| format!("{} {} '{}'", c.column_name, c.operator, c.value))
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            "none".to_string()
        }
    } else {
        "none".to_string()
    };
    let pushdown_cstr = CString::new(pushdown_desc).unwrap_or_default();
    pg_sys::ExplainPropertyText(label_pushdown.as_ptr(), pushdown_cstr.as_ptr(), es);

    let label_batch_unit = c"rows";
    pg_sys::ExplainPropertyInteger(
        label_batch.as_ptr(),
        label_batch_unit.as_ptr(),
        state.batch_size as i64,
        es,
    );

    if (*es).analyze {
        let label_rows = c"Rows Fetched";
        let label_rows_unit = c"rows";
        pg_sys::ExplainPropertyInteger(
            label_rows.as_ptr(),
            label_rows_unit.as_ptr(),
            state.row_count as i64,
            es,
        );
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn explain_foreign_modify(
    _mtstate: *mut pg_sys::ModifyTableState,
    rinfo: *mut pg_sys::ResultRelInfo,
    _fdw_private: *mut pg_sys::List,
    _subplan_index: ::core::ffi::c_int,
    es: *mut pg_sys::ExplainState,
) {
    log!("---> explain_foreign_modify");
    let fdw_state = (*rinfo).ri_FdwState as *mut RedisFdwState;
    if fdw_state.is_null() {
        return;
    }
    let state = &*fdw_state;

    let server_cstr = CString::new(state.host_port.as_str()).unwrap_or_default();
    let key_cstr = CString::new(state.table_key_prefix.as_str()).unwrap_or_default();
    let type_name = state.table_type.redis_type_name();
    let type_cstr = CString::new(type_name).unwrap_or_default();

    let label_server = c"Redis Server";
    let label_key = c"Redis Key";
    let label_type = c"Table Type";

    pg_sys::ExplainPropertyText(label_server.as_ptr(), server_cstr.as_ptr(), es);
    pg_sys::ExplainPropertyText(label_key.as_ptr(), key_cstr.as_ptr(), es);
    pg_sys::ExplainPropertyText(label_type.as_ptr(), type_cstr.as_ptr(), es);
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

#[pg_guard]
unsafe extern "C-unwind" fn exec_foreign_truncate(
    rels: *mut pg_sys::List,
    _behavior: pg_sys::DropBehavior::Type,
    _restart_seqs: bool,
) {
    log!("---> exec_foreign_truncate");
    use crate::core::connection_factory::{RedisConnectionConfig, RedisConnectionFactory};
    use crate::core::state_manager::is_multi_key_pattern;

    if rels.is_null() {
        return;
    }

    pgrx::memcx::current_context(|mcx| {
        let rel_list = pgrx::list::List::<*mut std::ffi::c_void>::downcast_ptr_in_memcx(rels, mcx)
            .expect("Failed to downcast rels list");

        for rel_ptr in rel_list.iter() {
            let relation = *rel_ptr as pg_sys::Relation;
            if relation.is_null() {
                continue;
            }
            let relid = (*relation).rd_id;
            let options = get_foreign_table_options(relid);

            let config = match RedisConnectionConfig::from_options(&options) {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to create Redis config for truncate: {}", e);
                }
            };

            let mut conn = match RedisConnectionFactory::create_connection_with_retry(&config) {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to connect to Redis for truncate: {}", e);
                }
            };

            let conn_like = conn.as_connection_like_mut();
            let key_prefix = options.get("table_key_prefix").cloned().unwrap_or_default();

            if is_multi_key_pattern(&key_prefix) {
                let mut cursor: u64 = 0;
                loop {
                    pgrx::check_for_interrupts!();
                    let (new_cursor, keys): (u64, Vec<String>) = match redis::cmd("SCAN")
                        .arg(cursor)
                        .arg("MATCH")
                        .arg(&key_prefix)
                        .arg("COUNT")
                        .arg(1000u32)
                        .query(conn_like)
                    {
                        Ok(r) => r,
                        Err(e) => {
                            error!("Redis SCAN error during truncate: {}", e);
                        }
                    };

                    if !keys.is_empty() {
                        let mut pipe = redis::pipe();
                        for key in &keys {
                            pipe.cmd("UNLINK").arg(key);
                        }
                        if let Err(e) = pipe.query::<Vec<redis::Value>>(conn_like) {
                            error!("Redis UNLINK pipeline failed during truncate: {}", e);
                        }
                    }

                    cursor = new_cursor;
                    if cursor == 0 {
                        break;
                    }
                }
            } else if let Err(e) = redis::cmd("UNLINK")
                .arg(&key_prefix)
                .query::<i64>(conn_like)
            {
                error!("Redis UNLINK failed for key '{}': {}", key_prefix, e);
            }
        }
    });
}

#[pg_guard]
unsafe extern "C-unwind" fn import_foreign_schema(
    stmt: *mut pg_sys::ImportForeignSchemaStmt,
    server_oid: pg_sys::Oid,
) -> *mut pg_sys::List {
    log!("---> import_foreign_schema");
    use crate::core::connection_factory::{RedisConnectionConfig, RedisConnectionFactory};
    use std::collections::HashMap as StdHashMap;

    let server = pg_sys::GetForeignServer(server_oid);
    let mut options: StdHashMap<String, String> = StdHashMap::new();

    pgrx::memcx::current_context(|mcx| {
        if !(*server).options.is_null() {
            let opts_list = pg_list_to_rust_list::<*mut std::ffi::c_void>((*server).options, mcx);
            for option in opts_list.iter() {
                let def_elem = (*option).cast::<pg_sys::DefElem>();
                if !def_elem.is_null() {
                    options.insert(
                        string_from_cstr((*def_elem).defname),
                        string_from_cstr(pg_sys::defGetString(def_elem)),
                    );
                }
            }
        }
    });

    let config = match RedisConnectionConfig::from_options(&options) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to create Redis config for import: {}", e);
        }
    };

    let mut conn = match RedisConnectionFactory::create_connection_with_retry(&config) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to connect to Redis for import: {}", e);
        }
    };

    let conn_like = conn.as_connection_like_mut();

    // SCAN to sample keys
    let mut all_keys: Vec<String> = Vec::new();
    let mut cursor: u64 = 0;
    let max_keys: usize = 10_000;
    loop {
        pgrx::check_for_interrupts!();
        let (new_cursor, keys): (u64, Vec<String>) = match redis::cmd("SCAN")
            .arg(cursor)
            .arg("COUNT")
            .arg(1000u32)
            .query(conn_like)
        {
            Ok(r) => r,
            Err(e) => {
                error!("Redis SCAN error during import: {}", e);
            }
        };

        all_keys.extend(keys);
        cursor = new_cursor;
        if cursor == 0 || all_keys.len() >= max_keys {
            break;
        }
    }

    if all_keys.len() > max_keys {
        all_keys.truncate(max_keys);
    }

    if all_keys.is_empty() {
        return ptr::null_mut();
    }

    // TYPE each key using pipeline
    let mut pipe = redis::pipe();
    for key in &all_keys {
        pipe.cmd("TYPE").arg(key);
    }
    let types: Vec<String> = match pipe.query(conn_like) {
        Ok(t) => t,
        Err(e) => {
            error!("Redis TYPE pipeline error during import: {}", e);
        }
    };

    // Group keys by prefix and type
    let mut groups: StdHashMap<String, String> = StdHashMap::new();
    for (key, redis_type) in all_keys.iter().zip(types.iter()) {
        if redis_type == "none" {
            continue;
        }
        let prefix = derive_key_prefix(key);
        groups.entry(prefix).or_insert_with(|| redis_type.clone());
    }

    // Build LIMIT TO / EXCEPT filter
    let list_type = (*stmt).list_type;
    let mut filter_names: Vec<String> = Vec::new();
    if !(*stmt).table_list.is_null() {
        pgrx::memcx::current_context(|mcx| {
            let table_list = pgrx::list::List::<*mut std::ffi::c_void>::downcast_ptr_in_memcx(
                (*stmt).table_list,
                mcx,
            )
            .expect("Failed to downcast table_list");
            for item in table_list.iter() {
                let rv = *item as *mut pg_sys::RangeVar;
                if !rv.is_null() && !(*rv).relname.is_null() {
                    filter_names.push(string_from_cstr((*rv).relname));
                }
            }
        });
    }

    // Generate DDL statements
    let server_name = string_from_cstr((*stmt).server_name);
    let mut result_list: *mut pg_sys::List = ptr::null_mut();

    for (prefix, redis_type) in &groups {
        let table_name = sanitize_table_name(prefix);

        match list_type {
            pg_sys::ImportForeignSchemaType::FDW_IMPORT_SCHEMA_LIMIT_TO
                if !filter_names.contains(&table_name) =>
            {
                continue;
            }
            pg_sys::ImportForeignSchemaType::FDW_IMPORT_SCHEMA_EXCEPT
                if filter_names.contains(&table_name) =>
            {
                continue;
            }
            _ => {}
        }

        let columns = columns_for_type(redis_type);
        let key_pattern = format!("{}*", prefix);
        let database_str = config.database.to_string();

        let quoted_table = table_name.replace('"', "\"\"");
        let quoted_server = server_name.replace('"', "\"\"");
        let escaped_prefix = key_pattern.replace('\'', "''");

        let ddl = format!(
            "CREATE FOREIGN TABLE \"{}\" ({}) SERVER \"{}\" OPTIONS (database '{}', table_type '{}', table_key_prefix '{}')",
            quoted_table, columns, quoted_server, database_str, redis_type, escaped_prefix
        );

        let ddl_cstr = match CString::new(ddl) {
            Ok(c) => c,
            Err(_) => {
                error!("import_foreign_schema: table name contains null byte");
            }
        };
        let pg_str = pg_sys::pstrdup(ddl_cstr.as_ptr());
        result_list = pg_sys::lappend(result_list, pg_str as *mut std::ffi::c_void);
    }

    result_list
}

fn derive_key_prefix(key: &str) -> String {
    if let Some(pos) = key.rfind(':') {
        key[..=pos].to_string()
    } else {
        format!("{}_", key)
    }
}

fn sanitize_table_name(prefix: &str) -> String {
    let mut name: String = prefix
        .trim_end_matches(':')
        .trim_end_matches('_')
        .replace([':', '-', '.'], "_")
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect();

    if name.starts_with(|c: char| c.is_ascii_digit()) {
        name = format!("t_{}", name);
    }

    if name.is_empty() {
        name = "redis_table".to_string();
    }

    if name.len() > 63 {
        name.truncate(63);
    }

    name
}

fn columns_for_type(redis_type: &str) -> &'static str {
    match redis_type {
        "hash" => "key text, field text, value text",
        "list" => "key text, element text",
        "set" => "key text, member text",
        "zset" => "key text, member text, score text",
        "string" => "key text, value text",
        "stream" => "stream_id text, field text, value text",
        _ => "value text",
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn analyze_foreign_table(
    relation: pg_sys::Relation,
    func: *mut pg_sys::AcquireSampleRowsFunc,
    totalpages: *mut pg_sys::BlockNumber,
) -> bool {
    log!("---> analyze_foreign_table");
    use crate::core::connection_factory::{RedisConnectionConfig, RedisConnectionFactory};
    use crate::core::state_manager::is_multi_key_pattern;

    let relid = (*relation).rd_id;
    let options = get_foreign_table_options(relid);

    let config = match RedisConnectionConfig::from_options(&options) {
        Ok(c) => c,
        Err(e) => {
            log!("analyze_foreign_table: cannot create config: {}", e);
            return false;
        }
    };

    let mut conn = match RedisConnectionFactory::create_connection_with_retry(&config) {
        Ok(c) => c,
        Err(e) => {
            log!("analyze_foreign_table: cannot connect: {}", e);
            return false;
        }
    };

    let conn_like = conn.as_connection_like_mut();
    let key_prefix = options.get("table_key_prefix").cloned().unwrap_or_default();
    let table_type = options
        .get("table_type")
        .map(|s| s.as_str())
        .unwrap_or("string");

    let estimated_rows: u64 = if is_multi_key_pattern(&key_prefix) {
        let mut cursor = 0u64;
        let mut total_keys = 0u64;
        let max_iterations = 100;
        let mut iterations = 0;
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&key_prefix)
                .arg("COUNT")
                .arg(10000u32)
                .query(conn_like)
                .unwrap_or((0, Vec::new()));
            total_keys += keys.len() as u64;
            cursor = next_cursor;
            iterations += 1;
            if cursor == 0 || iterations >= max_iterations {
                break;
            }
        }
        total_keys
    } else {
        match table_type {
            "hash" => redis::cmd("HLEN")
                .arg(&key_prefix)
                .query::<u64>(conn_like)
                .unwrap_or(0),
            "list" => redis::cmd("LLEN")
                .arg(&key_prefix)
                .query::<u64>(conn_like)
                .unwrap_or(0),
            "set" => redis::cmd("SCARD")
                .arg(&key_prefix)
                .query::<u64>(conn_like)
                .unwrap_or(0),
            "zset" => redis::cmd("ZCARD")
                .arg(&key_prefix)
                .query::<u64>(conn_like)
                .unwrap_or(0),
            "stream" => redis::cmd("XLEN")
                .arg(&key_prefix)
                .query::<u64>(conn_like)
                .unwrap_or(0),
            "string" => {
                let exists: u64 = redis::cmd("EXISTS")
                    .arg(&key_prefix)
                    .query(conn_like)
                    .unwrap_or(0);
                exists
            }
            _ => 0,
        }
    };

    let avg_row_width: u64 = 100;
    let pages =
        ((estimated_rows * avg_row_width) / pg_sys::BLCKSZ as u64).max(1) as pg_sys::BlockNumber;
    *totalpages = pages;
    *func = Some(acquire_sample_rows);
    true
}

#[pg_guard]
unsafe extern "C-unwind" fn acquire_sample_rows(
    relation: pg_sys::Relation,
    _elevel: ::core::ffi::c_int,
    rows: *mut pg_sys::HeapTuple,
    targrows: ::core::ffi::c_int,
    totalrows: *mut f64,
    totaldeadrows: *mut f64,
) -> ::core::ffi::c_int {
    log!("---> acquire_sample_rows (targrows={})", targrows);
    use crate::core::connection_factory::{RedisConnectionConfig, RedisConnectionFactory};
    use crate::core::state_manager::is_multi_key_pattern;
    use crate::query::limit::LimitOffsetInfo;

    let relid = (*relation).rd_id;
    let options = get_foreign_table_options(relid);
    let tupdesc = (*relation).rd_att;
    let natts = (*tupdesc).natts as usize;

    let config = match RedisConnectionConfig::from_options(&options) {
        Ok(c) => c,
        Err(e) => {
            log!("acquire_sample_rows: cannot create config: {}", e);
            *totalrows = 0.0;
            *totaldeadrows = 0.0;
            return 0;
        }
    };

    let mut conn = match RedisConnectionFactory::create_connection_with_retry(&config) {
        Ok(c) => c,
        Err(e) => {
            log!("acquire_sample_rows: cannot connect: {}", e);
            *totalrows = 0.0;
            *totaldeadrows = 0.0;
            return 0;
        }
    };

    let conn_like = conn.as_connection_like_mut();
    let key_prefix = options.get("table_key_prefix").cloned().unwrap_or_default();
    let table_type_str = options
        .get("table_type")
        .map(|s| s.as_str())
        .unwrap_or("string");

    let mut table_type = RedisTableType::from_str(table_type_str);
    let is_multi_key = is_multi_key_pattern(&key_prefix);

    let max_per_key = targrows as usize;
    let sample_data: Vec<Vec<String>> = if is_multi_key {
        let mut keys = Vec::new();
        let mut cursor = 0u64;
        loop {
            let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&key_prefix)
                .arg("COUNT")
                .arg(targrows as u32)
                .query(conn_like)
                .unwrap_or((0, Vec::new()));
            keys.extend(batch);
            cursor = next_cursor;
            if cursor == 0 || keys.len() >= max_per_key {
                break;
            }
        }
        keys.truncate(max_per_key);
        let mut result = Vec::new();

        if table_type_str == "string" {
            let mut pipe = redis::pipe();
            for key in &keys {
                pipe.cmd("GET").arg(key);
            }
            let vals: Vec<String> = pipe.query(conn_like).unwrap_or_default();
            for (key, val) in keys.iter().zip(vals.into_iter()) {
                result.push(vec![key.clone(), val]);
            }
        } else {
            for key in &keys {
                if result.len() >= max_per_key {
                    break;
                }
                let remaining = max_per_key - result.len();
                match table_type_str {
                    "hash" => {
                        let (_, vals): (u64, Vec<String>) = redis::cmd("HSCAN")
                            .arg(key)
                            .arg(0u64)
                            .arg("COUNT")
                            .arg(remaining)
                            .query(conn_like)
                            .unwrap_or((0, Vec::new()));
                        for chunk in vals.chunks(2).take(remaining) {
                            if chunk.len() == 2 {
                                result.push(vec![key.clone(), chunk[0].clone(), chunk[1].clone()]);
                            }
                        }
                    }
                    "list" => {
                        let vals: Vec<String> = redis::cmd("LRANGE")
                            .arg(key)
                            .arg(0i64)
                            .arg((remaining as i64) - 1)
                            .query(conn_like)
                            .unwrap_or_default();
                        for v in vals {
                            result.push(vec![key.clone(), v]);
                        }
                    }
                    "set" => {
                        let (_, vals): (u64, Vec<String>) = redis::cmd("SSCAN")
                            .arg(key)
                            .arg(0u64)
                            .arg("COUNT")
                            .arg(remaining)
                            .query(conn_like)
                            .unwrap_or((0, Vec::new()));
                        for v in vals.into_iter().take(remaining) {
                            result.push(vec![key.clone(), v]);
                        }
                    }
                    "zset" => {
                        let vals: Vec<String> = redis::cmd("ZRANGE")
                            .arg(key)
                            .arg(0i64)
                            .arg((remaining as i64) - 1)
                            .arg("WITHSCORES")
                            .query(conn_like)
                            .unwrap_or_default();
                        for chunk in vals.chunks(2) {
                            if chunk.len() == 2 {
                                result.push(vec![key.clone(), chunk[1].clone(), chunk[0].clone()]);
                            }
                        }
                    }
                    _ => {
                        result.push(vec![key.clone()]);
                    }
                }
            }
        }
        result
    } else {
        let limit_info = LimitOffsetInfo {
            limit: Some(targrows as usize),
            offset: None,
        };
        let _ = table_type.load_data(conn_like, &key_prefix, None, &limit_info);

        let mut result = Vec::new();
        let len = table_type.data_len();
        for i in 0..len {
            if let Some(row_data) = table_type.get_row(i) {
                result.push(row_data.into_iter().map(|c| c.into_owned()).collect());
            }
        }
        result
    };

    let num_rows = sample_data.len().min(targrows as usize);
    *totalrows = num_rows as f64;
    *totaldeadrows = 0.0;

    let mut actual = 0i32;
    for (idx, row_data) in sample_data.iter().take(num_rows).enumerate() {
        let mut values: Vec<pg_sys::Datum> = Vec::with_capacity(natts);
        let mut nulls: Vec<bool> = Vec::with_capacity(natts);

        for col_idx in 0..natts {
            if col_idx < row_data.len() {
                let attr = tuple_desc_attr(tupdesc, col_idx);
                let typid = (*attr).atttypid;
                let datum = get_datum(&row_data[col_idx], typid);
                values.push(datum);
                nulls.push(false);
            } else {
                values.push(pg_sys::Datum::from(0));
                nulls.push(true);
            }
        }

        let tuple = pg_sys::heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
        *rows.add(idx) = tuple;
        actual += 1;
    }

    actual
}

/// Extract join column indices from the join restrictlist.
/// Returns (outer_col_0based, inner_col_0based) if an equality condition is found
/// between a Var on the outer rel and a Var on the inner rel.
unsafe fn extract_join_columns(
    extra: *mut pgrx::pg_sys::JoinPathExtraData,
    outerrel: *mut pgrx::pg_sys::RelOptInfo,
    innerrel: *mut pgrx::pg_sys::RelOptInfo,
) -> Option<(usize, usize)> {
    if extra.is_null() {
        return None;
    }

    let restrictlist = (*extra).restrictlist;
    if restrictlist.is_null() {
        return None;
    }

    let outer_relids = (*outerrel).relids;
    let inner_relids = (*innerrel).relids;

    let list_length = pg_sys::list_length(restrictlist);
    for i in 0..list_length {
        let node = pg_sys::list_nth(restrictlist, i) as *mut pg_sys::Node;
        if node.is_null() {
            continue;
        }

        // Unwrap RestrictInfo wrapper
        let clause = if (*node).type_ == pg_sys::NodeTag::T_RestrictInfo {
            let ri = node as *mut pg_sys::RestrictInfo;
            (*ri).clause as *mut pg_sys::Node
        } else {
            node
        };

        if clause.is_null() || (*clause).type_ != pg_sys::NodeTag::T_OpExpr {
            continue;
        }

        let op_expr = &*(clause as *mut pg_sys::OpExpr);

        // Must be a merge-joinable (equality) operator with exactly 2 args
        if !pg_sys::op_mergejoinable(op_expr.opno, pg_sys::InvalidOid) {
            continue;
        }

        if pg_sys::list_length(op_expr.args) != 2 {
            continue;
        }

        let left_arg = pg_sys::list_nth(op_expr.args, 0) as *mut pg_sys::Node;
        let right_arg = pg_sys::list_nth(op_expr.args, 1) as *mut pg_sys::Node;

        if left_arg.is_null() || right_arg.is_null() {
            continue;
        }

        // Both args must be Var nodes
        if (*left_arg).type_ != pg_sys::NodeTag::T_Var
            || (*right_arg).type_ != pg_sys::NodeTag::T_Var
        {
            continue;
        }

        let left_var = &*(left_arg as *mut pg_sys::Var);
        let right_var = &*(right_arg as *mut pg_sys::Var);

        // Check which var belongs to outer/inner
        let left_in_outer = pg_sys::bms_is_member(left_var.varno as i32, outer_relids);
        let left_in_inner = pg_sys::bms_is_member(left_var.varno as i32, inner_relids);
        let right_in_outer = pg_sys::bms_is_member(right_var.varno as i32, outer_relids);
        let right_in_inner = pg_sys::bms_is_member(right_var.varno as i32, inner_relids);

        if left_in_outer && right_in_inner && left_var.varattno > 0 && right_var.varattno > 0 {
            let outer_col = (left_var.varattno - 1) as usize;
            let inner_col = (right_var.varattno - 1) as usize;
            return Some((outer_col, inner_col));
        } else if right_in_outer && left_in_inner && right_var.varattno > 0 && left_var.varattno > 0
        {
            let outer_col = (right_var.varattno - 1) as usize;
            let inner_col = (left_var.varattno - 1) as usize;
            return Some((outer_col, inner_col));
        }
    }

    None
}

#[pg_guard]
unsafe extern "C-unwind" fn get_foreign_join_paths(
    root: *mut pgrx::pg_sys::PlannerInfo,
    joinrel: *mut pgrx::pg_sys::RelOptInfo,
    outerrel: *mut pgrx::pg_sys::RelOptInfo,
    innerrel: *mut pgrx::pg_sys::RelOptInfo,
    jointype: pgrx::pg_sys::JoinType::Type,
    _extra: *mut pgrx::pg_sys::JoinPathExtraData,
) {
    log!("---> get_foreign_join_paths");

    if jointype != pgrx::pg_sys::JoinType::JOIN_INNER
        && jointype != pgrx::pg_sys::JoinType::JOIN_LEFT
    {
        log!("Unsupported join type for pushdown");
        return;
    }

    let outer_state_ptr = (*outerrel).fdw_private as *mut RedisFdwState;
    let inner_state_ptr = (*innerrel).fdw_private as *mut RedisFdwState;

    if outer_state_ptr.is_null() || inner_state_ptr.is_null() {
        log!("One or both relations lack FDW state, cannot push down join");
        return;
    }

    let outer_state = &*outer_state_ptr;
    let inner_state = &*inner_state_ptr;

    if outer_state.host_port != inner_state.host_port {
        log!(
            "Relations on different servers ({} vs {}), cannot push down",
            outer_state.host_port,
            inner_state.host_port
        );
        return;
    }

    // Multi-key pattern tables use SCAN-based iteration; pushdown not supported
    if outer_state.is_multi_key || inner_state.is_multi_key {
        log!("Multi-key pattern table detected, join pushdown not supported");
        return;
    }

    // Extract join columns from the restrictlist in JoinPathExtraData
    let (join_col_outer, join_col_inner) = match extract_join_columns(_extra, outerrel, innerrel) {
        Some(cols) => cols,
        None => {
            log!("No equality join clause found between FDW rels, cannot push down");
            return;
        }
    };

    log!(
        "Detected join columns: outer={}, inner={}",
        join_col_outer,
        join_col_inner
    );

    let outer_rows = (*outerrel).rows;
    let inner_rows = (*innerrel).rows;
    let startup_cost = 11.0;
    let total_cost =
        startup_cost + (outer_rows.min(inner_rows) * 0.01) + (outer_rows.max(inner_rows) * 0.0025);

    let joinrel_rows = outer_rows.min(inner_rows);

    // Store outer+inner state pointers, jointype, and join columns
    let fdw_private = serialize_join_info_to_list(
        outer_state_ptr as *mut std::os::raw::c_void,
        inner_state_ptr as *mut std::os::raw::c_void,
        jointype as i64,
        join_col_outer as i64,
        join_col_inner as i64,
    );

    let path = pgrx::pg_sys::create_foreign_join_path(
        root,
        joinrel,
        ptr::null_mut(),
        joinrel_rows,
        #[cfg(feature = "pg18")]
        0,
        startup_cost,
        total_cost,
        ptr::null_mut(),
        (*joinrel).lateral_relids,
        ptr::null_mut(),
        #[cfg(any(feature = "pg17", feature = "pg18"))]
        ptr::null_mut(),
        fdw_private,
    );
    pgrx::pg_sys::add_path(joinrel, path as *mut pgrx::pg_sys::Path);
}
