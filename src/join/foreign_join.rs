use crate::join::types::{RedisJoinState, RedisJoinType};
use crate::tables::types::RedisTableType;
use std::collections::HashMap;

const MAX_JOIN_DATASET_ROWS: usize = 500_000;

pub fn execute_foreign_join(state: &mut RedisJoinState) -> usize {
    let conn = match state.connection.as_mut() {
        Some(c) => c.as_connection_like_mut(),
        None => {
            pgrx::error!("Redis FDW: no connection available for join execution");
        }
    };

    let outer_data = fetch_dataset(conn, &state.outer_table_type, &state.outer_key_prefix);
    let inner_data = fetch_dataset(conn, &state.inner_table_type, &state.inner_key_prefix);

    if outer_data.len() > MAX_JOIN_DATASET_ROWS || inner_data.len() > MAX_JOIN_DATASET_ROWS {
        pgrx::warning!(
            "Redis FDW: join materializing large datasets (outer={}, inner={})",
            outer_data.len(),
            inner_data.len()
        );
    }

    let (build_data, probe_data, build_col, probe_col, build_is_outer) =
        if outer_data.len() <= inner_data.len() {
            (
                &outer_data,
                &inner_data,
                state.join_column_outer,
                state.join_column_inner,
                true,
            )
        } else {
            (
                &inner_data,
                &outer_data,
                state.join_column_inner,
                state.join_column_outer,
                false,
            )
        };

    let mut hash_table: HashMap<&str, Vec<usize>> = HashMap::new();
    for (idx, row) in build_data.iter().enumerate() {
        if let Some(key) = row.get(build_col) {
            hash_table.entry(key.as_str()).or_default().push(idx);
        }
    }

    let outer_cols = expected_columns_for_type(&state.outer_table_type);
    let inner_cols = expected_columns_for_type(&state.inner_table_type);

    let mut result: Vec<Vec<String>> = Vec::new();
    let mut matched_build: Vec<bool> = vec![false; build_data.len()];

    // Pre-allocate null padding row outside the loop for LEFT JOIN
    let null_pad_row = if state.join_type == RedisJoinType::Left && !build_is_outer {
        let null_cols = if build_is_outer {
            inner_cols
        } else {
            outer_cols
        };
        vec!["NULL".to_string(); null_cols]
    } else {
        Vec::new()
    };

    for probe_row in probe_data {
        if let Some(probe_key) = probe_row.get(probe_col) {
            if let Some(build_indices) = hash_table.get(probe_key.as_str()) {
                for &build_idx in build_indices {
                    matched_build[build_idx] = true;
                    let combined = if build_is_outer {
                        combine_rows(&build_data[build_idx], probe_row)
                    } else {
                        combine_rows(probe_row, &build_data[build_idx])
                    };
                    result.push(combined);
                }
            } else if state.join_type == RedisJoinType::Left && !build_is_outer {
                let combined = combine_rows(probe_row, &null_pad_row);
                result.push(combined);
            }
        }
    }

    if state.join_type == RedisJoinType::Left && build_is_outer {
        let null_inner = vec!["NULL".to_string(); inner_cols];
        for (idx, matched) in matched_build.iter().enumerate() {
            if !matched {
                let combined = combine_rows(&build_data[idx], &null_inner);
                result.push(combined);
            }
        }
    }

    let count = result.len();
    state.result_columns = result.first().map_or(0, |r| r.len());
    state.result_data = result;
    state.current_row = 0;
    count
}

fn fetch_dataset(
    conn: &mut dyn redis::ConnectionLike,
    table_type: &RedisTableType,
    key_prefix: &str,
) -> Vec<Vec<String>> {
    match table_type {
        RedisTableType::Hash(_) => {
            let pairs: Vec<(String, String)> = redis::cmd("HGETALL")
                .arg(key_prefix)
                .query(conn)
                .unwrap_or_else(|e| {
                    pgrx::error!("Redis FDW: HGETALL '{}' failed: {}", key_prefix, e);
                });
            pairs.into_iter().map(|(f, v)| vec![f, v]).collect()
        }
        RedisTableType::Set(_) => {
            let members: Vec<String> = redis::cmd("SMEMBERS")
                .arg(key_prefix)
                .query(conn)
                .unwrap_or_else(|e| {
                    pgrx::error!("Redis FDW: SMEMBERS '{}' failed: {}", key_prefix, e);
                });
            members.into_iter().map(|m| vec![m]).collect()
        }
        RedisTableType::ZSet(_) => {
            let items: Vec<(String, f64)> = redis::cmd("ZRANGE")
                .arg(key_prefix)
                .arg(0i64)
                .arg(-1i64)
                .arg("WITHSCORES")
                .query(conn)
                .unwrap_or_else(|e| {
                    pgrx::error!("Redis FDW: ZRANGE '{}' failed: {}", key_prefix, e);
                });
            items
                .into_iter()
                .map(|(member, score)| vec![member, score.to_string()])
                .collect()
        }
        RedisTableType::List(_) => {
            let items: Vec<String> = redis::cmd("LRANGE")
                .arg(key_prefix)
                .arg(0i64)
                .arg(-1i64)
                .query(conn)
                .unwrap_or_else(|e| {
                    pgrx::error!("Redis FDW: LRANGE '{}' failed: {}", key_prefix, e);
                });
            items.into_iter().map(|v| vec![v]).collect()
        }
        RedisTableType::String(_) => {
            let val: Option<String> = redis::cmd("GET")
                .arg(key_prefix)
                .query(conn)
                .unwrap_or_else(|e| {
                    pgrx::error!("Redis FDW: GET '{}' failed: {}", key_prefix, e);
                });
            match val {
                Some(v) => vec![vec![key_prefix.to_string(), v]],
                None => vec![],
            }
        }
        _ => vec![],
    }
}

fn expected_columns_for_type(table_type: &RedisTableType) -> usize {
    match table_type {
        RedisTableType::Hash(_) => 2,
        RedisTableType::Set(_) => 1,
        RedisTableType::ZSet(_) => 2,
        RedisTableType::List(_) => 1,
        RedisTableType::String(_) => 2,
        _ => 1,
    }
}

fn combine_rows(outer: &[String], inner: &[String]) -> Vec<String> {
    let mut combined = Vec::with_capacity(outer.len() + inner.len());
    combined.extend_from_slice(outer);
    combined.extend_from_slice(inner);
    combined
}
