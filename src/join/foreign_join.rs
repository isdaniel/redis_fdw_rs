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

    let inner_cols = expected_columns_for_type(&state.inner_table_type);

    let mut result: Vec<Vec<Option<String>>> = Vec::new();
    let mut matched_build: Vec<bool> = vec![false; build_data.len()];

    let null_pad_row: Vec<Option<String>> =
        if state.join_type == RedisJoinType::Left && !build_is_outer {
            vec![None; inner_cols]
        } else {
            Vec::new()
        };

    for probe_row in probe_data {
        if let Some(probe_key) = probe_row.get(probe_col) {
            if let Some(build_indices) = hash_table.get(probe_key.as_str()) {
                for &build_idx in build_indices {
                    matched_build[build_idx] = true;
                    let combined = if build_is_outer {
                        to_option_row(&build_data[build_idx], probe_row)
                    } else {
                        to_option_row(probe_row, &build_data[build_idx])
                    };
                    result.push(combined);
                }
            } else if state.join_type == RedisJoinType::Left && !build_is_outer {
                let mut combined = to_some_vec(probe_row);
                combined.extend_from_slice(&null_pad_row);
                result.push(combined);
            }
        } else if state.join_type == RedisJoinType::Left && !build_is_outer {
            let mut combined = to_some_vec(probe_row);
            combined.extend_from_slice(&null_pad_row);
            result.push(combined);
        }
    }

    if state.join_type == RedisJoinType::Left && build_is_outer {
        let null_inner: Vec<Option<String>> = vec![None; inner_cols];
        for (idx, matched) in matched_build.iter().enumerate() {
            if !matched {
                let mut combined = to_some_vec(&build_data[idx]);
                combined.extend_from_slice(&null_inner);
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
    // Pre-check cardinality to avoid OOM from unbounded HGETALL/SMEMBERS
    let cardinality: u64 = match table_type {
        RedisTableType::Hash(_) => redis::cmd("HLEN").arg(key_prefix).query(conn),
        RedisTableType::Set(_) => redis::cmd("SCARD").arg(key_prefix).query(conn),
        RedisTableType::ZSet(_) => redis::cmd("ZCARD").arg(key_prefix).query(conn),
        RedisTableType::List(_) => redis::cmd("LLEN").arg(key_prefix).query(conn),
        RedisTableType::Stream(_) => redis::cmd("XLEN").arg(key_prefix).query(conn),
        _ => Ok(0),
    }
    .unwrap_or_else(|e| {
        pgrx::error!(
            "Redis FDW: failed to get cardinality for key '{}': {}",
            key_prefix,
            e
        )
    });
    if cardinality > MAX_JOIN_DATASET_ROWS as u64 {
        pgrx::error!(
            "Redis FDW: join dataset '{}' has {} elements, exceeding limit of {}. Use a WHERE clause to reduce data volume.",
            key_prefix,
            cardinality,
            MAX_JOIN_DATASET_ROWS
        );
    }

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

pub fn expected_columns_for_type(table_type: &RedisTableType) -> usize {
    match table_type {
        RedisTableType::Hash(_) => 2,
        RedisTableType::Set(_) => 1,
        RedisTableType::ZSet(_) => 2,
        RedisTableType::List(_) => 1,
        RedisTableType::String(_) => 2,
        RedisTableType::Stream(_) => 3,
        RedisTableType::None => 0,
    }
}

fn to_option_row(outer: &[String], inner: &[String]) -> Vec<Option<String>> {
    let mut combined = Vec::with_capacity(outer.len() + inner.len());
    for s in outer {
        combined.push(Some(s.clone()));
    }
    for s in inner {
        combined.push(Some(s.clone()));
    }
    combined
}

fn to_some_vec(data: &[String]) -> Vec<Option<String>> {
    data.iter().map(|s| Some(s.clone())).collect()
}
