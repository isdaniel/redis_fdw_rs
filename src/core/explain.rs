use crate::core::state_manager::RedisFdwState;
use pgrx::prelude::*;
use std::ffi::CString;

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn explain_foreign_scan(
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
pub(crate) unsafe extern "C-unwind" fn explain_foreign_modify(
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
