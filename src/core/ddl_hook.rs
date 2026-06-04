use pgrx::pg_guard;
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

#[pg_guard]
unsafe extern "C-unwind" fn redis_fdw_object_access_hook(
    access: pg_sys::ObjectAccessType::Type,
    class_id: pg_sys::Oid,
    object_id: pg_sys::Oid,
    sub_id: std::os::raw::c_int,
    arg: *mut std::ffi::c_void,
) {
    if let Some(prev) = PREV_OBJECT_ACCESS_HOOK {
        // Fn-pointer dispatch is not auto-wrapped by pgrx (only named pg_sys::* symbols are).
        // Without this guard, a longjmp from another extension's hook would skip the Rust frame and bypass Drop on any locals.
        pgrx::pg_sys::ffi::pg_guard_ffi_boundary(|| unsafe {
            prev(access, class_id, object_id, sub_id, arg)
        });
    }

    if access != pg_sys::ObjectAccessType::OAT_POST_CREATE {
        return;
    }

    if class_id == pg_sys::ForeignTableRelationId {
        // No inner pgrx_extern_c_guard needed — the outer #[pg_guard] on this function already wraps the body in one, so a second layer is redundant.
        validate_foreign_table_columns(object_id);
    }
}

/// Validate a freshly-created foreign table's column count.
///
/// Structured in phases so that all Rust heap allocations (HashMap, String) are dropped *before* `validate_column_count` is reached. `validate_column_count` raises `pgrx::error!()` on mismatch, which becomes a `siglongjmp` and skips Rust Drop on any locals still in scope — so we make sure nothing leakable is in scope at that point.
unsafe fn validate_foreign_table_columns(rel_id: pg_sys::Oid) {
    if !is_our_foreign_table(rel_id) {
        return;
    }

    let Some((table_type, is_multi_key)) = extract_validation_inputs(rel_id) else {
        return;
    };

    let data_column_count = count_data_columns(rel_id);

    // At this point only `table_type` (small enum) + primitives are alive.
    // No HashMap, no owned String. A panic here leaks nothing on the Rust heap.
    validate_column_count(&table_type, data_column_count, is_multi_key);
}

/// Phase 1: is this foreign table backed by `redis_fdw_handler`?
///
/// All allocations (`handler_name: String`, plus the palloc'd C string) are
/// confined to this function's scope and freed before return.
unsafe fn is_our_foreign_table(rel_id: pg_sys::Oid) -> bool {
    let ft = pg_sys::GetForeignTable(rel_id);
    if ft.is_null() {
        return false;
    }
    let server = pg_sys::GetForeignServer((*ft).serverid);
    if server.is_null() {
        return false;
    }
    let fdw = pg_sys::GetForeignDataWrapper((*server).fdwid);
    if fdw.is_null() {
        return false;
    }
    if (*fdw).fdwhandler == pg_sys::InvalidOid {
        return false;
    }

    let handler_name_ptr = pg_sys::get_func_name((*fdw).fdwhandler);
    if handler_name_ptr.is_null() {
        return false;
    }
    let is_match = std::ffi::CStr::from_ptr(handler_name_ptr).to_bytes() == b"redis_fdw_handler";
    pg_sys::pfree(handler_name_ptr as *mut std::ffi::c_void);
    is_match
}

/// Phase 2: read table options and resolve into a validated `(type, multi_key)` pair.
///
/// The `HashMap<String, String>` returned by `get_foreign_table_options` lives only
/// inside this function; it's dropped on return. Only the cheap `RedisTableType`
/// enum + `bool` escape.
unsafe fn extract_validation_inputs(rel_id: pg_sys::Oid) -> Option<(RedisTableType, bool)> {
    let opts = get_foreign_table_options(rel_id);

    let table_type_str = opts.get("table_type")?;
    let table_type = RedisTableType::from_str(table_type_str);
    if matches!(table_type, RedisTableType::None) {
        return None;
    }

    let is_multi_key = opts
        .get("table_key_prefix")
        .map(|p| is_multi_key_pattern(p))
        .unwrap_or(false);

    Some((table_type, is_multi_key))
}

/// Phase 3: open the relation, count non-TTL data columns, close.
///
/// The lock is released before return — no PG lock is held across the eventual
/// `pgrx::error!()` call site.
unsafe fn count_data_columns(rel_id: pg_sys::Oid) -> usize {
    let rel = pg_sys::relation_open(rel_id, pg_sys::AccessShareLock as i32);
    let tupdesc = (*rel).rd_att;
    let natts = (*tupdesc).natts as usize;

    let mut count = 0usize;
    for i in 0..natts {
        let attr = crate::utils::helpers::tuple_desc_attr(tupdesc, i);
        if (*attr).attisdropped {
            continue;
        }
        let name = pgrx::name_data_to_str(&(*attr).attname);
        if name.eq_ignore_ascii_case("ttl") {
            continue;
        }
        count += 1;
    }

    pg_sys::relation_close(rel, pg_sys::AccessShareLock as i32);
    count
}
