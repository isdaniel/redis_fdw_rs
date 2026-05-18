use crate::{
    core::state_manager::RedisFdwState,
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

        fdw_routine.AddForeignUpdateTargets = Some(add_foreign_update_targets);
        fdw_routine.PlanForeignModify = Some(plan_foreign_modify);
        fdw_routine.BeginForeignModify = Some(begin_foreign_modify);
        fdw_routine.ExecForeignInsert = Some(exec_foreign_insert);
        fdw_routine.ExecForeignDelete = Some(exec_foreign_delete);
        fdw_routine.ExecForeignUpdate = Some(exec_foreign_update);
        fdw_routine.EndForeignModify = Some(end_foreign_modify);
        fdw_routine.IsForeignRelUpdatable = Some(is_foreign_rel_updatable);

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

        // Calculate cost estimate using actual Redis statistics
        let cost_estimate = state.estimate_costs();
        log!(
            "Cost estimation: rows={}, startup_cost={}, total_cost={}, width={}",
            cost_estimate.rows,
            cost_estimate.startup_cost,
            cost_estimate.total_cost,
            cost_estimate.width
        );

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
    _best_path: *mut pgrx::pg_sys::ForeignPath,
    tlist: *mut pgrx::pg_sys::List,
    scan_clauses: *mut pgrx::pg_sys::List,
    outer_plan: *mut pgrx::pg_sys::Plan,
) -> *mut pgrx::pg_sys::ForeignScan {
    log!("---> get_foreign_plan");

    // Get the FDW state from baserel to analyze table type
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

#[pg_guard]
extern "C-unwind" fn begin_foreign_scan(
    node: *mut pgrx::pg_sys::ForeignScanState,
    _eflags: ::std::os::raw::c_int,
) {
    log!("---> begin_foreign_scan");
    unsafe {
        let scan_state = (*node).ss;
        let plan: *mut pg_sys::ForeignScan = scan_state.ps.plan as *mut pg_sys::ForeignScan;
        let relation = (*node).ss.ss_currentRelation;
        let relid = (*relation).rd_id;
        let state_ptr = deserialize_ptr_from_list((*plan).fdw_private as _);
        let state = state_from_ptr(state_ptr);
        PgMemoryContexts::For(state.tmp_ctx).switch_to(|_| {
            let options = get_foreign_table_options(relid);
            log!("Foreign table options: {:?}", options);
            state.update_from_options(options);

            // Initialize Redis connection and handle potential errors
            if let Err(e) = state.init_redis_connection_from_options() {
                pgrx::error!("Failed to connect to Redis: {}", e);
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

#[pg_guard]
unsafe extern "C-unwind" fn iterate_foreign_scan(
    node: *mut pgrx::pg_sys::ForeignScanState,
) -> *mut pgrx::pg_sys::TupleTableSlot {
    log!("---> iterate_foreign_scan");
    let state = state_from_ptr((*node).fdw_state);
    let slot = (*node).ss.ss_ScanTupleSlot;
    let tupdesc = (*slot).tts_tupleDescriptor;

    exec_clear_tuple(slot);

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

        // Reset iteration and streaming state for rescan
        state.row_count = 0;
        state.scan_cursor = 0;
        state.scan_complete = false;
        state.cached_ttl = None;
        state.multi_key_ttl_cache.clear();
        state.table_type.clear_data();
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
                // Multi-key mode: DEL the entire Redis key
                if let Some(conn) = state.redis_connection.as_mut() {
                    let conn_like = conn.as_connection_like_mut();
                    let result: Result<(), _> = redis::cmd("DEL").arg(&key).query(conn_like);
                    if let Err(e) = result {
                        error!("Failed to delete Redis key '{}': {:?}", key, e);
                    }
                } else {
                    error!("Redis connection not available for delete");
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
