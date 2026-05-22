use pgrx::pg_sys;

use crate::core::column_utils::validate_column_count;
use crate::core::state_manager::is_multi_key_pattern;
use crate::tables::types::RedisTableType;
use crate::utils::helpers::get_foreign_table_options;

static mut PREV_OBJECT_ACCESS_HOOK: pg_sys::object_access_hook_type = None;

pub unsafe fn init_hook() {
    PREV_OBJECT_ACCESS_HOOK = pg_sys::object_access_hook;
    pg_sys::object_access_hook = Some(redis_fdw_object_access_hook);
}

unsafe extern "C-unwind" fn redis_fdw_object_access_hook(
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_id: pg_sys::Oid,
    sub_id: std::os::raw::c_int,
    arg: *mut std::ffi::c_void,
) {
    if let Some(prev) = PREV_OBJECT_ACCESS_HOOK {
        prev(access, class_id, object_id, sub_id, arg);
    }

    if access != pg_sys::ObjectAccessType::OAT_POST_CREATE {
        return;
    }

    if class_id == pg_sys::ForeignTableRelationId {
        pgrx::pgrx_extern_c_guard(|| {
            validate_foreign_table_columns(object_id);
        });
    }
}

unsafe fn validate_foreign_table_columns(rel_id: pg_sys::Oid) {
    let ft = pg_sys::GetForeignTable(rel_id);
    if ft.is_null() {
        return;
    }
    let server = pg_sys::GetForeignServer((*ft).serverid);
    if server.is_null() {
        return;
    }
    let fdw = pg_sys::GetForeignDataWrapper((*server).fdwid);
    if fdw.is_null() {
        return;
    }

    if (*fdw).fdwhandler == pg_sys::InvalidOid {
        return;
    }

    let handler_name_ptr = pg_sys::get_func_name((*fdw).fdwhandler);
    if handler_name_ptr.is_null() {
        return;
    }
    let handler_name = std::ffi::CStr::from_ptr(handler_name_ptr)
        .to_string_lossy()
        .into_owned();
    pg_sys::pfree(handler_name_ptr as *mut std::ffi::c_void);

    if handler_name != "redis_fdw_handler" {
        return;
    }

    let opts = get_foreign_table_options(rel_id);

    let table_type_str = match opts.get("table_type") {
        Some(tt) => tt.clone(),
        None => return,
    };
    let table_type = RedisTableType::from_str(&table_type_str);
    if matches!(table_type, RedisTableType::None) {
        return;
    }

    let is_multi_key = opts
        .get("table_key_prefix")
        .map(|p| is_multi_key_pattern(p))
        .unwrap_or(false);

    let rel = pg_sys::relation_open(rel_id, pg_sys::AccessShareLock as i32);
    let tupdesc = (*rel).rd_att;
    let natts = (*tupdesc).natts as usize;

    let mut data_column_count = 0usize;
    for i in 0..natts {
        let attr = crate::utils::helpers::tuple_desc_attr(tupdesc, i);
        if (*attr).attisdropped {
            continue;
        }
        let name = pgrx::name_data_to_str(&(*attr).attname);
        if name.eq_ignore_ascii_case("ttl") {
            continue;
        }
        data_column_count += 1;
    }

    pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);

    validate_column_count(&table_type, data_column_count, is_multi_key);
}
