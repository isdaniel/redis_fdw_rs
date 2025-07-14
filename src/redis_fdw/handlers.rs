use std::{collections::HashMap, ffi::c_int, ptr, slice};
use pgrx::{ pg_sys::{ CmdType, Datum, Index, MemoryContextData, ModifyTable, PlannerInfo}, prelude::*, AllocatedByRust, PgBox, PgMemoryContexts, PgRelation, PgTupleDesc
};
use redis::Commands;
use crate::{redis_fdw::state::{RedisFdwState, RedisModifyFdwState}, utils_share::{
    cell::Cell,
    memory::create_wrappers_memctx,
    row::Row,
    utils::{
        self, build_attr_name_to_index_map, delete_wrappers_memctx, deserialize_from_list, exec_clear_tuple, find_rowid_column, get_datum, get_foreign_table_options, serialize_to_list, tuple_desc_attr, tuple_table_slot_to_row
    }
}};

pub type FdwRoutine<A = AllocatedByRust> = PgBox<pgrx::pg_sys::FdwRoutine, A>;

#[pg_extern(create_or_replace)]
pub extern "C" fn redis_fdw_handler() -> FdwRoutine {
    log!("---> redis_fdw_handler");
    unsafe {
        let mut fdw_routine = PgBox::<pgrx::pg_sys::FdwRoutine, AllocatedByRust>::alloc_node(pgrx::pg_sys::NodeTag::T_FdwRoutine);

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
        //fdw_routine.IsForeignRelUpdatable =

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
        let state = RedisFdwState::new(ctx);

        (*baserel).fdw_private = Box::into_raw(Box::new(state)) as *mut RedisFdwState as *mut std::os::raw::c_void;
        (*baserel).rows = 1000.0;
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
        let path = pgrx::pg_sys::create_foreignscan_path(
            _root,
            baserel,
            ptr::null_mut(),           
            (*baserel).rows,            
            10.0,                       
            100.0,                     
            ptr::null_mut(),            
            ptr::null_mut(),            
            ptr::null_mut(),           
            ptr::null_mut(),           
        );
        pgrx::pg_sys::add_path(baserel, path as *mut pgrx::pg_sys::Path);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn get_foreign_plan(
    _root: *mut pgrx::pg_sys::PlannerInfo,
    baserel: *mut pgrx::pg_sys::RelOptInfo,
    _foreigntableid: pgrx::pg_sys::Oid,
    _best_path: *mut pgrx::pg_sys::ForeignPath,
    tlist: *mut pgrx::pg_sys::List,
    scan_clauses: *mut pgrx::pg_sys::List,
    outer_plan: *mut pgrx::pg_sys::Plan,
) -> *mut pgrx::pg_sys::ForeignScan {
    log!("---> get_foreign_plan");
    pgrx::pg_sys::make_foreignscan(
        tlist,
        pg_sys::extract_actual_clauses(scan_clauses, false),
        (*baserel).relid,
        ptr::null_mut(),
        (*baserel).fdw_private as _,
        ptr::null_mut(),
        ptr::null_mut(),
        outer_plan,
    )
}

#[pg_guard]
extern "C-unwind" fn begin_foreign_scan(
    node: *mut pgrx::pg_sys::ForeignScanState,
    eflags: ::std::os::raw::c_int,
) {

    log!("---> begin_foreign_scan");
    unsafe {
        let scan_state = (*node).ss;
        let plan: *mut pg_sys::ForeignScan = scan_state.ps.plan as *mut pg_sys::ForeignScan;
        let relation = (*node).ss.ss_currentRelation;
        let relid = (*relation).rd_id;
        let mut state =  PgBox::<RedisFdwState>::from_pg((*plan).fdw_private as _); 

        let options = get_foreign_table_options(relid);
        log!("Foreign table options: {:?}", options);

        state.header_name_to_colno = build_attr_name_to_index_map(relation);
        log!("Header name to column number mapping: {:?}", state.header_name_to_colno);
        // Connect to Redis and handle potential errors
        state.redis_connection = init_redis_connection_from_options(&options);
        state.table_type = options.get("table_type").unwrap_or(&"String".to_string()).to_string();
        state.table_key_prefix = options.get("table_key_prefix").unwrap().to_string();
        log!("Connected to Redis");
        (*node).fdw_state = state.into_pg() as _;
    }
}

#[pg_guard]
extern "C-unwind" fn iterate_foreign_scan(
    node: *mut pgrx::pg_sys::ForeignScanState,
) -> *mut pgrx::pg_sys::TupleTableSlot {
    log!("---> iterate_foreign_scan");

    unsafe {
        let mut state = PgBox::<RedisFdwState>::from_pg((*node).fdw_state as _);
        let slot = (*node).ss.ss_ScanTupleSlot;
        let tupdesc = (*slot).tts_tupleDescriptor;
        
        exec_clear_tuple(slot);
        if state.is_read {
            return slot;
        }
        //todo support table_type Hash, Set, List, ZSet in future.
        let table_key_prefix = state.table_key_prefix.clone();
        info!("Fetching data from Redis for key prefix: {}", table_key_prefix);
        let conn = state.redis_connection.as_mut().expect("Redis connection is not established");
        let map: HashMap<String, String> = conn.hgetall(table_key_prefix).expect("Failed to fetch data from Redis");
        
        for (col_name, value_str) in map.iter() {
            let colno = state.header_name_to_colno[col_name];
            let pgtype = (*tuple_desc_attr(tupdesc, colno )).atttypid;
            let datum_value = get_datum(value_str, pgtype);
            (*slot).tts_values.add(colno).write(datum_value);
            (*slot).tts_isnull.add(colno).write(false);
        }
        state.is_read = true;
        pgrx::pg_sys::ExecStoreVirtualTuple(slot);
        slot
    }
}

#[pg_guard]
extern "C-unwind" fn end_foreign_scan(
    node: *mut pgrx::pg_sys::ForeignScanState,
) {
    log!("---> end_foreign_scan");
    unsafe {
        let fdw_state = (*node).fdw_state as *mut RedisFdwState;
        if fdw_state.is_null() {
            return;
        }
        let mut state = PgBox::<RedisFdwState>::from_pg(fdw_state);
        delete_wrappers_memctx(state.tmp_ctx);
        state.tmp_ctx = ptr::null::<MemoryContextData>() as _;
        
        let _ = Box::from_raw(fdw_state);
    }
}

#[pg_guard]
extern "C-unwind" fn re_scan_foreign_scan(
    _node: *mut pgrx::pg_sys::ForeignScanState,
) {
    log!("---> re_scan_foreign_scan");
    // Reset or reinitialize scan state here if needed
}

#[pg_guard]
unsafe extern "C-unwind" fn add_foreign_update_targets(
    root: *mut pgrx::pg_sys::PlannerInfo,
    rtindex: pgrx::pg_sys::Index,
    _target_rte: *mut pgrx::pg_sys::RangeTblEntry,
    target_relation: pgrx::pg_sys::Relation,
) {
 
    log!("---> add_foreign_update_targets");
    if let Some(attr) = find_rowid_column(target_relation) {
        // make a Var representing the desired value
        let var = pg_sys::makeVar(
            rtindex as _,
            attr.attnum,
            attr.atttypid,
            attr.atttypmod,
            attr.attcollation,
            0,
        );

        // register it as a row-identity column needed by this target rel
        pg_sys::add_row_identity_var(root, var, rtindex, &attr.attname.data as _);
    }
}


#[pg_guard]
unsafe extern "C-unwind" fn plan_foreign_modify(
    root: *mut PlannerInfo,
    plan: *mut ModifyTable,
    result_relation: Index,
    _subplan_index: ::core::ffi::c_int,
) -> *mut pgrx::pg_sys::List {
    log!("---> plan_foreign_modify");
    let rte = pg_sys::planner_rt_fetch(result_relation, root);
    let rel = PgRelation::with_lock((*rte).relid, pg_sys::NoLock as _);
    // search for rowid attribute in tuple descrition
    let tup_desc = PgTupleDesc::from_relation(&rel);
    let ftable_id = rel.oid();
    let ctx_name = format!("Wrappers_modify_{}", ftable_id.to_u32());
    let ctx = create_wrappers_memctx(&ctx_name);
    let mut state: RedisModifyFdwState = RedisModifyFdwState::new(ctx);
    state.opts = get_foreign_table_options(ftable_id);
    info!("Foreign table options for modify: {:?}", state.opts);
    state.redis_connection = init_redis_connection_from_options(&state.opts);
    state.table_key_prefix = state.opts.get("table_key_prefix").unwrap().to_string();
    let p: *mut RedisModifyFdwState = Box::leak(Box::new(state)) as *mut RedisModifyFdwState;
    let state: PgBox<RedisModifyFdwState> = PgBox::<RedisModifyFdwState>::from_pg(p as _);
    serialize_to_list(state)
}

#[pg_guard]
unsafe extern "C-unwind" fn begin_foreign_modify(
    mtstate: *mut pgrx::pg_sys::ModifyTableState,
    rinfo: *mut pgrx::pg_sys::ResultRelInfo,
    fdw_private: *mut pgrx::pg_sys::List,
    subplan_index: ::std::os::raw::c_int,
    eflags: ::std::os::raw::c_int,
) {
    log!("---> begin_foreign_modify");
    let state = deserialize_from_list::<RedisModifyFdwState>(fdw_private as _);
    (*rinfo).ri_FdwState = state.into_pg() as *mut std::os::raw::c_void;
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
    let mut state = PgBox::<RedisModifyFdwState>::from_pg((*rinfo).ri_FdwState as _);
    let table_key_prefix = state.table_key_prefix.clone();
    let conn = state.redis_connection.as_mut().expect("Redis connection is not established");
    let row: Row = tuple_table_slot_to_row(slot);
    for i in 0..row.cells.len() {
        let cell = &row.cells[i];
        let col_name = &row.cols[i];
        let val = match cell {
            Some(c) => c.to_string(),
            None => "NULL".to_string(),
        };
        info!(
            "Inserted column: {}, value: {}, table_key_prefix: {}",
            col_name.to_string(),
            val,
            table_key_prefix
        );
        let _: () = conn.hset(&table_key_prefix, col_name, val).expect("Failed to set Redis hash field");
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
    slot
}

#[pg_guard]
unsafe extern "C-unwind" fn end_foreign_modify(
    _estate: *mut pgrx::pg_sys::EState,
    rinfo: *mut pgrx::pg_sys::ResultRelInfo,
) {
    log!("---> end_foreign_modify");
}

fn init_redis_connection_from_options(options: &HashMap<String, String>) -> Option<redis::Connection> {
    let host_port = options.get("host_port").expect("Missing 'host_port' option for Redis foreign table");
    let database = options.get("database").and_then(|db_str| db_str.parse::<i64>().ok()).unwrap_or(0);
    let addr_port = format!("redis://{}/{}" ,host_port, database);
    let client = redis::Client::open(addr_port).expect("Failed to create Redis client");
    Some(client.get_connection().expect("Failed to connect to Redis"))
}
