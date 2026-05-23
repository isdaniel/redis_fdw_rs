use crate::{
    core::{
        column_utils::{
            adjust_column_for_ttl_strip, compute_pushdown_column_index, state_from_ptr,
        },
        state_manager::RedisFdwState,
    },
    join::types::{RedisJoinState, RedisJoinType},
    query::cost_estimation::costs,
    tables::types::RedisTableType,
    utils::{helpers::*, memory::create_wrappers_memctx},
};
use pgrx::{prelude::*, PgMemoryContexts};
use std::ptr;

pub(crate) unsafe fn add_parameterized_paths(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    state: &RedisFdwState,
) {
    let my_relids = (*baserel).relids;

    let pushdown_col = compute_pushdown_column_index(state.ttl_column_index, false);

    if (*baserel).has_eclass_joins {
        let ec_list = (*root).eq_classes;
        if !ec_list.is_null() {
            let ec_len = pg_sys::list_length(ec_list);
            for i in 0..ec_len {
                let ec = pg_sys::list_nth(ec_list, i) as *mut pg_sys::EquivalenceClass;
                if ec.is_null() {
                    continue;
                }

                let ec_members = (*ec).ec_members;
                if ec_members.is_null() {
                    continue;
                }

                let mut has_my_pushdown_col = false;
                let mut outer_relids_for_ec: *mut pg_sys::Bitmapset = ptr::null_mut();

                let mem_len = pg_sys::list_length(ec_members);
                for j in 0..mem_len {
                    let em = pg_sys::list_nth(ec_members, j) as *mut pg_sys::EquivalenceMember;
                    if em.is_null() {
                        continue;
                    }

                    let em_expr = (*em).em_expr as *mut pg_sys::Node;
                    if em_expr.is_null() {
                        continue;
                    }
                    if (*em_expr).type_ != pg_sys::NodeTag::T_Var {
                        continue;
                    }

                    let var = &*(em_expr as *mut pg_sys::Var);

                    if pg_sys::bms_is_member(var.varno as i32, my_relids) {
                        let col_idx = (var.varattno - 1) as usize;
                        if col_idx == pushdown_col {
                            has_my_pushdown_col = true;
                        }
                    } else if var.varno > 0 {
                        outer_relids_for_ec =
                            pg_sys::bms_add_member(outer_relids_for_ec, var.varno as i32);
                    }
                }

                if has_my_pushdown_col && !outer_relids_for_ec.is_null() {
                    let param_path = pg_sys::create_foreignscan_path(
                        root,
                        baserel,
                        ptr::null_mut(),
                        1.0,
                        #[cfg(feature = "pg18")]
                        0,
                        costs::CPU_TUPLE_COST,
                        costs::PARAMETERIZED_LOOKUP_COST,
                        ptr::null_mut(),
                        outer_relids_for_ec,
                        ptr::null_mut(),
                        #[cfg(any(feature = "pg17", feature = "pg18"))]
                        ptr::null_mut(),
                        ptr::null_mut(),
                    );
                    pg_sys::add_path(baserel, param_path as *mut pg_sys::Path);
                    log!(
                        "Added EC parameterized path for pushdown col {} with outer relids",
                        pushdown_col
                    );
                }
            }
        }
    }

    let joininfo = (*baserel).joininfo;
    if joininfo.is_null() {
        return;
    }

    let list_len = pg_sys::list_length(joininfo);

    for i in 0..list_len {
        let cell = pg_sys::list_nth(joininfo, i) as *mut pg_sys::RestrictInfo;
        if cell.is_null() {
            continue;
        }

        let ri = &*cell;
        let clause = ri.clause as *mut pg_sys::Node;
        if clause.is_null() || (*clause).type_ != pg_sys::NodeTag::T_OpExpr {
            continue;
        }

        let op_expr = &*(clause as *mut pg_sys::OpExpr);
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
        if (*left_arg).type_ != pg_sys::NodeTag::T_Var
            || (*right_arg).type_ != pg_sys::NodeTag::T_Var
        {
            continue;
        }

        let left_var = &*(left_arg as *mut pg_sys::Var);
        let right_var = &*(right_arg as *mut pg_sys::Var);

        let (my_col_attno, outer_relid) = if pg_sys::bms_is_member(left_var.varno as i32, my_relids)
            && !pg_sys::bms_is_member(right_var.varno as i32, my_relids)
        {
            (left_var.varattno, right_var.varno)
        } else if pg_sys::bms_is_member(right_var.varno as i32, my_relids)
            && !pg_sys::bms_is_member(left_var.varno as i32, my_relids)
        {
            (right_var.varattno, left_var.varno)
        } else {
            continue;
        };

        if my_col_attno <= 0 {
            continue;
        }
        let my_col_idx = (my_col_attno - 1) as usize;

        let valid = match &state.table_type {
            RedisTableType::Hash(_) => my_col_idx == pushdown_col,
            RedisTableType::Set(_) => my_col_idx == pushdown_col,
            RedisTableType::ZSet(_) => my_col_idx == pushdown_col,
            RedisTableType::String(_) if state.is_multi_key => my_col_idx == pushdown_col,
            _ => false,
        };
        if !valid {
            continue;
        }

        #[allow(clippy::unnecessary_cast)]
        let required_outer = pg_sys::bms_make_singleton(outer_relid as i32);

        let param_path = pg_sys::create_foreignscan_path(
            root,
            baserel,
            ptr::null_mut(),
            1.0,
            #[cfg(feature = "pg18")]
            0,
            costs::CPU_TUPLE_COST,
            costs::PARAMETERIZED_LOOKUP_COST,
            ptr::null_mut(),
            required_outer,
            ptr::null_mut(),
            #[cfg(any(feature = "pg17", feature = "pg18"))]
            ptr::null_mut(),
            ptr::null_mut(),
        );
        pg_sys::add_path(baserel, param_path as *mut pg_sys::Path);
        log!(
            "Added parameterized path for col {} with outer relid {}",
            my_col_idx,
            outer_relid
        );
    }
}

pub(crate) unsafe fn plan_foreign_join(
    _root: *mut pgrx::pg_sys::PlannerInfo,
    joinrel: *mut pgrx::pg_sys::RelOptInfo,
    best_path: *mut pgrx::pg_sys::ForeignPath,
    tlist: *mut pgrx::pg_sys::List,
    scan_clauses: *mut pgrx::pg_sys::List,
    outer_plan: *mut pgrx::pg_sys::Plan,
) -> *mut pgrx::pg_sys::ForeignScan {
    log!("---> plan_foreign_join");

    let path_private = (*best_path).fdw_private;
    let outer_state_ptr = deserialize_nth_ptr_from_list(path_private, 0) as *mut RedisFdwState;
    let inner_state_ptr = deserialize_nth_ptr_from_list(path_private, 1) as *mut RedisFdwState;

    if outer_state_ptr.is_null() || inner_state_ptr.is_null() {
        pgrx::error!("Join plan: missing outer/inner state in fdw_private");
    }

    let outer_state = &*outer_state_ptr;
    let inner_state = &*inner_state_ptr;

    let jointype_val = deserialize_nth_ptr_from_list(path_private, 2) as i64;
    let join_type = if jointype_val == pgrx::pg_sys::JoinType::JOIN_LEFT as i64 {
        RedisJoinType::Left
    } else {
        RedisJoinType::Inner
    };

    let join_col_outer = deserialize_nth_ptr_from_list(path_private, 3) as usize;
    let join_col_inner = deserialize_nth_ptr_from_list(path_private, 4) as usize;

    let outer_relid = deserialize_nth_ptr_from_list(path_private, 5) as pg_sys::Index;
    let inner_relid = deserialize_nth_ptr_from_list(path_private, 6) as pg_sys::Index;

    let join_state = RedisJoinState::new(
        outer_state.table_type.clone(),
        inner_state.table_type.clone(),
        outer_state.table_key_prefix.clone(),
        inner_state.table_key_prefix.clone(),
        join_type,
        join_col_outer,
        join_col_inner,
    );

    let ctx_name = "Wrappers_join_scan";
    let ctx = create_wrappers_memctx(ctx_name);
    let mut state = RedisFdwState::new(ctx);

    state.host_port = outer_state.host_port.clone();
    state.database = outer_state.database;
    state.opts = outer_state.opts.clone();
    state.is_join_scan = true;
    state.join_state = Some(join_state);

    let state_ptr = PgMemoryContexts::For(ctx).leak_and_drop_on_delete(state);
    (*joinrel).fdw_private = state_ptr as *mut std::os::raw::c_void;

    let outer_ncols = crate::join::foreign_join::expected_columns_for_type(&outer_state.table_type);
    let inner_ncols = crate::join::foreign_join::expected_columns_for_type(&inner_state.table_type);

    let mut fdw_scan_tlist: *mut pg_sys::List = ptr::null_mut();
    let mut resno: i16 = 1;

    for attno in 1..=(outer_ncols as i16) {
        let var = pg_sys::makeVar(
            outer_relid as _,
            attno,
            pg_sys::TEXTOID,
            -1,
            pg_sys::DEFAULT_COLLATION_OID,
            0,
        );
        let tle = pg_sys::makeTargetEntry(var as *mut pg_sys::Expr, resno, ptr::null_mut(), false);
        fdw_scan_tlist = pg_sys::lappend(fdw_scan_tlist, tle as *mut std::ffi::c_void);
        resno += 1;
    }
    for attno in 1..=(inner_ncols as i16) {
        let var = pg_sys::makeVar(
            inner_relid as _,
            attno,
            pg_sys::TEXTOID,
            -1,
            pg_sys::DEFAULT_COLLATION_OID,
            0,
        );
        let tle = pg_sys::makeTargetEntry(var as *mut pg_sys::Expr, resno, ptr::null_mut(), false);
        fdw_scan_tlist = pg_sys::lappend(fdw_scan_tlist, tle as *mut std::ffi::c_void);
        resno += 1;
    }

    let fdw_private = serialize_ptr_to_list(state_ptr as *mut std::os::raw::c_void);
    let local_quals = pg_sys::extract_actual_clauses(scan_clauses, false);

    pgrx::pg_sys::make_foreignscan(
        tlist,
        local_quals,
        0,
        ptr::null_mut(),
        fdw_private as _,
        fdw_scan_tlist,
        ptr::null_mut(),
        outer_plan,
    )
}

pub(crate) unsafe fn begin_foreign_join_scan(
    node: *mut pgrx::pg_sys::ForeignScanState,
    plan: *mut pg_sys::ForeignScan,
) {
    log!("---> begin_foreign_join_scan");
    let state_ptr = deserialize_ptr_from_list((*plan).fdw_private as _);
    let state = state_from_ptr(state_ptr);

    if state.redis_connection.is_none() {
        if let Err(e) = state.init_redis_connection_from_options() {
            pgrx::error!("Failed to connect to Redis for join scan: {}", e);
        }
    }

    if let Some(ref mut join_state) = state.join_state {
        join_state.connection = state.redis_connection.take();
    }

    (*node).fdw_state = state_ptr;
}

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

    let restrict_items: Vec<*mut pg_sys::Node> = pgrx::memcx::current_context(|mcx| {
        let list = pg_list_to_rust_list::<*mut std::ffi::c_void>(restrictlist, mcx);
        list.iter().map(|item| *item as *mut pg_sys::Node).collect()
    });

    for node in &restrict_items {
        let node = *node;
        if node.is_null() {
            continue;
        }

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

        if (*left_arg).type_ != pg_sys::NodeTag::T_Var
            || (*right_arg).type_ != pg_sys::NodeTag::T_Var
        {
            continue;
        }

        let left_var = &*(left_arg as *mut pg_sys::Var);
        let right_var = &*(right_arg as *mut pg_sys::Var);

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
pub(crate) unsafe extern "C-unwind" fn get_foreign_join_paths(
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

    if outer_state.is_multi_key || inner_state.is_multi_key {
        log!("Multi-key pattern table detected, join pushdown not supported");
        return;
    }

    if matches!(outer_state.table_type, RedisTableType::Stream(_))
        || matches!(inner_state.table_type, RedisTableType::Stream(_))
    {
        log!("Stream table detected, join pushdown not supported");
        return;
    }

    if (matches!(outer_state.table_type, RedisTableType::String(_)) && !outer_state.is_multi_key)
        || (matches!(inner_state.table_type, RedisTableType::String(_))
            && !inner_state.is_multi_key)
    {
        log!("Single-key String table detected, join pushdown not supported");
        return;
    }

    if !(*outerrel).baserestrictinfo.is_null()
        && pg_sys::list_length((*outerrel).baserestrictinfo) > 0
    {
        log!("Outer relation has base restrictions, skipping join pushdown");
        return;
    }
    if !(*innerrel).baserestrictinfo.is_null()
        && pg_sys::list_length((*innerrel).baserestrictinfo) > 0
    {
        log!("Inner relation has base restrictions, skipping join pushdown");
        return;
    }

    let (join_col_outer, join_col_inner) = match extract_join_columns(_extra, outerrel, innerrel) {
        Some(cols) => cols,
        None => {
            log!("No equality join clause found between FDW rels, cannot push down");
            return;
        }
    };

    let join_col_outer =
        match adjust_column_for_ttl_strip(join_col_outer, outer_state.ttl_column_index) {
            Some(col) => col,
            None => {
                log!("Join condition targets TTL column on outer relation, cannot push down");
                return;
            }
        };
    let join_col_inner =
        match adjust_column_for_ttl_strip(join_col_inner, inner_state.ttl_column_index) {
            Some(col) => col,
            None => {
                log!("Join condition targets TTL column on inner relation, cannot push down");
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

    let network_cost = costs::NETWORK_ROUND_TRIP * 4.0
        + (outer_rows + inner_rows) * costs::NETWORK_TRANSFER_PER_ROW;
    let build_cost = inner_rows.min(outer_rows) * costs::CPU_TUPLE_COST;
    let probe_cost = inner_rows.max(outer_rows) * costs::CPU_TUPLE_COST;
    let startup_cost = network_cost + build_cost;
    let total_cost = startup_cost + probe_cost;

    let joinrel_rows = outer_rows.min(inner_rows);

    let fdw_private = serialize_join_info_to_list(&[
        outer_state_ptr as i64,
        inner_state_ptr as i64,
        jointype as i64,
        join_col_outer as i64,
        join_col_inner as i64,
        (*outerrel).relid as i64,
        (*innerrel).relid as i64,
    ]);

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
