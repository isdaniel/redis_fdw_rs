use crate::{
    core::connection_factory::{RedisConnectionConfig, RedisConnectionFactory},
    core::state_manager::is_multi_key_pattern,
    query::limit::LimitOffsetInfo,
    tables::types::RedisTableType,
    utils::helpers::*,
};
use pgrx::prelude::*;
use std::ffi::CString;
use std::ptr;

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn import_foreign_schema(
    stmt: *mut pg_sys::ImportForeignSchemaStmt,
    server_oid: pg_sys::Oid,
) -> *mut pg_sys::List {
    log!("---> import_foreign_schema");
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

    let remote_schema = string_from_cstr((*stmt).remote_schema);
    let scan_pattern = if is_multi_key_pattern(&remote_schema) {
        Some(remote_schema)
    } else {
        None
    };

    let mut all_keys: Vec<String> = Vec::new();
    let mut cursor: u64 = 0;
    let max_keys: usize = 10_000;
    loop {
        pgrx::check_for_interrupts!();
        let mut cmd = redis::cmd("SCAN");
        cmd.arg(cursor);
        if let Some(ref pattern) = scan_pattern {
            cmd.arg("MATCH").arg(pattern.as_str());
        }
        cmd.arg("COUNT").arg(1000u32);
        let (new_cursor, keys): (u64, Vec<String>) = match cmd.query(conn_like) {
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

    let mut types: Vec<String> = Vec::with_capacity(all_keys.len());
    for chunk in all_keys.chunks(1000) {
        let mut pipe = redis::pipe();
        for key in chunk {
            pipe.cmd("TYPE").arg(key);
        }
        let chunk_types: Vec<String> = match pipe.query(conn_like) {
            Ok(t) => t,
            Err(e) => {
                error!("Redis TYPE pipeline error during import: {}", e);
            }
        };
        types.extend(chunk_types);
    }

    let mut groups: StdHashMap<String, String> = StdHashMap::new();
    for (key, redis_type) in all_keys.iter().zip(types.iter()) {
        if redis_type == "none" {
            continue;
        }
        let prefix = if redis_type == "stream" {
            key.clone()
        } else {
            derive_key_prefix(key)
        };
        groups.entry(prefix).or_insert_with(|| redis_type.clone());
    }

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

    let server_name = string_from_cstr((*stmt).server_name);
    let mut result_list: *mut pg_sys::List = ptr::null_mut();
    let mut table_count = 0usize;
    const MAX_IMPORT_TABLES: usize = 1000;

    for (prefix, redis_type) in &groups {
        if table_count >= MAX_IMPORT_TABLES {
            pgrx::warning!(
                "redis_fdw: import_foreign_schema stopped at {} tables (limit reached)",
                MAX_IMPORT_TABLES
            );
            break;
        }

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
        let key_pattern = if redis_type == "stream" {
            prefix.clone()
        } else {
            format!("{}*", prefix)
        };
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
                pgrx::warning!(
                    "redis_fdw: skipping key prefix '{}' (contains null byte)",
                    prefix.replace('\0', "\\0")
                );
                continue;
            }
        };
        let pg_str = pg_sys::pstrdup(ddl_cstr.as_ptr());
        result_list = pg_sys::lappend(result_list, pg_str as *mut std::ffi::c_void);
        table_count += 1;
    }

    result_list
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn analyze_foreign_table(
    relation: pg_sys::Relation,
    func: *mut pg_sys::AcquireSampleRowsFunc,
    totalpages: *mut pg_sys::BlockNumber,
) -> bool {
    log!("---> analyze_foreign_table");

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
pub(crate) unsafe extern "C-unwind" fn acquire_sample_rows(
    relation: pg_sys::Relation,
    _elevel: ::core::ffi::c_int,
    rows: *mut pg_sys::HeapTuple,
    targrows: ::core::ffi::c_int,
    totalrows: *mut f64,
    totaldeadrows: *mut f64,
) -> ::core::ffi::c_int {
    log!("---> acquire_sample_rows (targrows={})", targrows);

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
            const PIPE_BATCH: usize = 1000;
            match table_type_str {
                "hash" => {
                    for batch in keys.chunks(PIPE_BATCH) {
                        if result.len() >= max_per_key {
                            break;
                        }
                        let mut pipe = redis::pipe();
                        for key in batch {
                            pipe.cmd("HSCAN")
                                .arg(key)
                                .arg(0u64)
                                .arg("COUNT")
                                .arg(100u64);
                        }
                        let results: Vec<(u64, Vec<String>)> =
                            pipe.query(conn_like).unwrap_or_default();
                        for (key, (_, vals)) in batch.iter().zip(results) {
                            for chunk in vals.chunks(2) {
                                if chunk.len() == 2 && result.len() < max_per_key {
                                    result.push(vec![
                                        key.clone(),
                                        chunk[0].clone(),
                                        chunk[1].clone(),
                                    ]);
                                }
                            }
                        }
                    }
                }
                "list" => {
                    for batch in keys.chunks(PIPE_BATCH) {
                        if result.len() >= max_per_key {
                            break;
                        }
                        let mut pipe = redis::pipe();
                        for key in batch {
                            let remaining = (max_per_key - result.len()).min(100) as i64;
                            pipe.cmd("LRANGE").arg(key).arg(0i64).arg(remaining - 1);
                        }
                        let results: Vec<Vec<String>> = pipe.query(conn_like).unwrap_or_default();
                        for (key, vals) in batch.iter().zip(results) {
                            for v in vals {
                                if result.len() >= max_per_key {
                                    break;
                                }
                                result.push(vec![key.clone(), v]);
                            }
                        }
                    }
                }
                "set" => {
                    for batch in keys.chunks(PIPE_BATCH) {
                        if result.len() >= max_per_key {
                            break;
                        }
                        let mut pipe = redis::pipe();
                        for key in batch {
                            pipe.cmd("SSCAN")
                                .arg(key)
                                .arg(0u64)
                                .arg("COUNT")
                                .arg(100u64);
                        }
                        let results: Vec<(u64, Vec<String>)> =
                            pipe.query(conn_like).unwrap_or_default();
                        for (key, (_, vals)) in batch.iter().zip(results) {
                            for v in vals {
                                if result.len() >= max_per_key {
                                    break;
                                }
                                result.push(vec![key.clone(), v]);
                            }
                        }
                    }
                }
                "zset" => {
                    for batch in keys.chunks(PIPE_BATCH) {
                        if result.len() >= max_per_key {
                            break;
                        }
                        let mut pipe = redis::pipe();
                        for key in batch {
                            pipe.cmd("ZRANGE")
                                .arg(key)
                                .arg(0i64)
                                .arg(99i64)
                                .arg("WITHSCORES");
                        }
                        let results: Vec<Vec<String>> = pipe.query(conn_like).unwrap_or_default();
                        for (key, vals) in batch.iter().zip(results) {
                            for chunk in vals.chunks(2) {
                                if chunk.len() == 2 && result.len() < max_per_key {
                                    result.push(vec![
                                        key.clone(),
                                        chunk[0].clone(),
                                        chunk[1].clone(),
                                    ]);
                                }
                            }
                        }
                    }
                }
                _ => {
                    for key in &keys {
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

fn derive_key_prefix(key: &str) -> String {
    if let Some(pos) = key.rfind(':') {
        key[..=pos].to_string()
    } else {
        key.to_string()
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
