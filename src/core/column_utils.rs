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

/// Compute the raw attribute index of the first data column for pushdown.
///
/// The "first data column" is the column that HSCAN MATCH / ZSCAN MATCH / XRANGE should target. It accounts for:
/// - Multi-key mode: the key-prefix column occupies the first non-TTL position
/// - TTL column: may appear before the data columns, shifting their raw position
pub(crate) fn compute_pushdown_column_index(
    ttl_column_index: Option<usize>,
    is_multi_key: bool,
) -> usize {
    let logical_position = if is_multi_key { 1 } else { 0 };
    match ttl_column_index {
        Some(ttl_idx) if ttl_idx <= logical_position => logical_position + 1,
        _ => logical_position,
    }
}

/// Convert a raw PostgreSQL attribute index to a data-row index after TTL column stripping.
///
/// `fetch_dataset` in join execution returns rows without the TTL column.
/// This adjusts the raw `varattno - 1` index so it points at the correct
/// position in the TTL-stripped data row.
pub(crate) fn adjust_column_for_ttl_strip(col: usize, ttl_idx: Option<usize>) -> Option<usize> {
    match ttl_idx {
        Some(t) if t == col => None,
        Some(t) if t < col => Some(col - 1),
        _ => Some(col),
    }
}

pub(crate) unsafe fn datum_to_text_string(datum: pg_sys::Datum, typoid: pg_sys::Oid) -> String {
    if typoid == pg_sys::TEXTOID || typoid == pg_sys::VARCHAROID || typoid == pg_sys::BPCHAROID {
        String::from_datum(datum, false).unwrap_or_default()
    } else {
        let mut out_func_oid: pg_sys::Oid = pg_sys::InvalidOid;
        let mut is_varlena = false;
        pg_sys::getTypeOutputInfo(typoid, &mut out_func_oid, &mut is_varlena);
        if out_func_oid == pg_sys::InvalidOid {
            pgrx::error!(
                "redis_fdw: could not find output function for type OID {}",
                typoid
            );
        }
        let cstr = pg_sys::OidOutputFunctionCall(out_func_oid, datum);
        if cstr.is_null() {
            return String::new();
        }
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
        RedisTableType::List(_) => {
            if is_multi_key {
                (2, 2, "list", "key, element")
            } else {
                (1, 2, "list", "element [, index]")
            }
        }
        RedisTableType::Set(_) => (1 + extra, 1 + extra, "set", "member"),
        RedisTableType::ZSet(_) => (2 + extra, 2 + extra, "zset", "member, score"),
        RedisTableType::Stream(_) => {
            if is_multi_key {
                pgrx::error!("redis_fdw: multi-key mode is not supported for stream tables");
            }
            (2, usize::MAX, "stream", "stream_id, field1[, ...]")
        }
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
        } else if max_cols == usize::MAX {
            pgrx::error!(
                "redis_fdw: table type '{}' requires at least {} data column(s) ({}){}, but foreign table has {}. \
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_pushdown_column_index() {
        // Normal mode, no TTL
        assert_eq!(compute_pushdown_column_index(None, false), 0);
        // Normal mode, TTL at position 0 (before data)
        assert_eq!(compute_pushdown_column_index(Some(0), false), 1);
        // Normal mode, TTL after data columns
        assert_eq!(compute_pushdown_column_index(Some(2), false), 0);
        // Multi-key, no TTL: key at 0, data at 1
        assert_eq!(compute_pushdown_column_index(None, true), 1);
        // Multi-key, TTL at 0: TTL at 0, key at 1, data at 2
        assert_eq!(compute_pushdown_column_index(Some(0), true), 2);
        // Multi-key, TTL at 1: key at 0, TTL at 1, data at 2
        assert_eq!(compute_pushdown_column_index(Some(1), true), 2);
        // Multi-key, TTL after data columns
        assert_eq!(compute_pushdown_column_index(Some(3), true), 1);
    }

    #[test]
    fn test_adjust_column_for_ttl_strip() {
        // No TTL column
        assert_eq!(adjust_column_for_ttl_strip(0, None), Some(0));
        assert_eq!(adjust_column_for_ttl_strip(1, None), Some(1));
        // TTL at position 0, accessing column 1 → becomes 0 in stripped data
        assert_eq!(adjust_column_for_ttl_strip(1, Some(0)), Some(0));
        assert_eq!(adjust_column_for_ttl_strip(2, Some(0)), Some(1));
        // TTL at position 0, accessing column 0 → None (targeting the TTL column itself)
        assert_eq!(adjust_column_for_ttl_strip(0, Some(0)), None);
        // TTL at position 1, accessing column 0 → unchanged
        assert_eq!(adjust_column_for_ttl_strip(0, Some(1)), Some(0));
        // TTL at position 1, accessing column 1 → None (targeting TTL)
        assert_eq!(adjust_column_for_ttl_strip(1, Some(1)), None);
        // TTL at position 1, accessing column 2 → becomes 1
        assert_eq!(adjust_column_for_ttl_strip(2, Some(1)), Some(1));
        // TTL after the target column
        assert_eq!(adjust_column_for_ttl_strip(0, Some(3)), Some(0));
        assert_eq!(adjust_column_for_ttl_strip(1, Some(3)), Some(1));
    }
}
