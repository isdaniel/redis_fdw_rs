use crate::{core::state_manager::RedisFdwState, tables::types::RedisTableType};
use pgrx::prelude::*;

#[inline]
pub(crate) unsafe fn state_from_ptr<'a>(ptr: *mut std::os::raw::c_void) -> &'a mut RedisFdwState {
    if ptr.is_null() {
        pgrx::error!("Redis FDW state pointer is null");
    }
    &mut *(ptr as *mut RedisFdwState)
}

pub(crate) unsafe fn detect_ttl_column(tupdesc: pg_sys::TupleDesc) -> Option<usize> {
    use crate::utils::helpers::tuple_desc_attr;
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

pub(crate) unsafe fn extract_column_names(tupdesc: pg_sys::TupleDesc) -> Vec<String> {
    use crate::utils::helpers::tuple_desc_attr;
    let natts = (*tupdesc).natts as usize;
    let mut names = Vec::with_capacity(natts);
    for i in 0..natts {
        let attr = tuple_desc_attr(tupdesc, i);
        if (*attr).attisdropped {
            continue;
        }
        let name = pgrx::name_data_to_str(&(*attr).attname);
        names.push(name.to_string());
    }
    names
}

pub(crate) unsafe fn datum_to_text_string(
    datum: pg_sys::Datum,
    col_idx: usize,
    tupdesc: pg_sys::TupleDesc,
) -> String {
    use crate::utils::helpers::tuple_desc_attr;

    let attr = tuple_desc_attr(tupdesc, col_idx);
    let typoid = (*attr).atttypid;

    if typoid == pg_sys::TEXTOID || typoid == pg_sys::VARCHAROID || typoid == pg_sys::BPCHAROID {
        let text_ptr = datum.cast_mut_ptr::<pg_sys::varlena>();
        if text_ptr.is_null() {
            return String::new();
        }
        let detoasted = pg_sys::pg_detoast_datum_packed(text_ptr);
        let cstr = pg_sys::text_to_cstring(detoasted);
        let result = std::ffi::CStr::from_ptr(cstr)
            .to_string_lossy()
            .into_owned();
        pg_sys::pfree(cstr as *mut std::ffi::c_void);
        if detoasted != text_ptr {
            pg_sys::pfree(detoasted as *mut std::ffi::c_void);
        }
        result
    } else {
        let mut out_func_oid: pg_sys::Oid = pg_sys::InvalidOid;
        let mut is_varlena = false;
        pg_sys::getTypeOutputInfo(typoid, &mut out_func_oid, &mut is_varlena);
        let cstr = pg_sys::OidOutputFunctionCall(out_func_oid, datum);
        let result = std::ffi::CStr::from_ptr(cstr)
            .to_string_lossy()
            .into_owned();
        pg_sys::pfree(cstr as *mut std::ffi::c_void);
        result
    }
}

pub(crate) fn validate_column_count(
    table_type: &RedisTableType,
    column_count: usize,
    is_multi_key: bool,
) {
    let extra = if is_multi_key { 1 } else { 0 };
    let (min_cols, max_cols, type_name, expected_desc) = match table_type {
        RedisTableType::String(_) => (1 + extra, 1 + extra, "string", "value"),
        RedisTableType::Hash(_) => (2 + extra, 2 + extra, "hash", "field, value"),
        RedisTableType::List(_) => (1 + extra, 2 + extra, "list", "element [, index]"),
        RedisTableType::Set(_) => (1 + extra, 1 + extra, "set", "member"),
        RedisTableType::ZSet(_) => (2 + extra, 2 + extra, "zset", "member, score"),
        RedisTableType::Stream(_) => (2, usize::MAX, "stream", "stream_id, field1[, ...]"),
        RedisTableType::None => return,
    };

    if column_count < min_cols || column_count > max_cols {
        let multi_key_note = if is_multi_key {
            " (including key column for multi-key mode)"
        } else {
            ""
        };
        if min_cols == max_cols {
            pgrx::error!(
                "redis_fdw: table type '{}' requires exactly {} data column(s) ({}){}, but foreign table has {}. \
                 Exclude the optional 'ttl' column from this count.",
                type_name,
                min_cols,
                expected_desc,
                multi_key_note,
                column_count
            );
        } else {
            pgrx::error!(
                "redis_fdw: table type '{}' requires {}-{} data column(s) ({}){}, but foreign table has {}. \
                 Exclude the optional 'ttl' column from this count.",
                type_name,
                min_cols,
                max_cols,
                expected_desc,
                multi_key_note,
                column_count
            );
        }
    }
}

pub(crate) fn transform_insert_data(
    table_type: &RedisTableType,
    column_names: &[String],
    data: Vec<String>,
) -> Vec<String> {
    match table_type {
        RedisTableType::List(_) if column_names.len() >= 2 => data.into_iter().skip(1).collect(),
        RedisTableType::Stream(_) if column_names.len() > 1 => {
            let mut stream_data = Vec::with_capacity(1 + (data.len() - 1) * 2);
            stream_data.push(data[0].clone());
            for (i, val) in data[1..].iter().enumerate() {
                if let Some(col_name) = column_names.get(i + 1) {
                    stream_data.push(col_name.clone());
                    stream_data.push(val.clone());
                }
            }
            stream_data
        }
        _ => data,
    }
}

pub(crate) unsafe fn extract_delete_key(
    state: &RedisFdwState,
    plan_slot: *mut pgrx::pg_sys::TupleTableSlot,
) -> Result<String, &'static str> {
    use crate::utils::helpers::exec_get_junk_attribute;

    if state.key_attno <= 0 {
        return Err("Invalid key attribute number");
    }

    let mut is_null = false;
    let datum = exec_get_junk_attribute(plan_slot, state.key_attno, &mut is_null);

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
