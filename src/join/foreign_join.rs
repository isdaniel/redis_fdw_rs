use crate::join::types::{JoinResultRow, JoinRow, RedisJoinState, RedisJoinType};
use crate::tables::types::RedisTableType;
use smallvec::smallvec;
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

    let result = perform_hash_join(
        &outer_data,
        &inner_data,
        state.join_column_outer,
        state.join_column_inner,
        &state.join_type,
    );

    let count = result.len();
    state.result_columns = expected_columns_for_type(&state.outer_table_type)
        + expected_columns_for_type(&state.inner_table_type);
    state.outer_data = outer_data;
    state.inner_data = inner_data;
    state.result_indices = result;
    state.current_row = 0;
    count
}

pub(crate) fn perform_hash_join(
    outer_data: &[JoinRow],
    inner_data: &[JoinRow],
    join_column_outer: usize,
    join_column_inner: usize,
    join_type: &RedisJoinType,
) -> Vec<JoinResultRow> {
    let (build_data, probe_data, build_col, probe_col, build_is_outer) =
        if outer_data.len() <= inner_data.len() {
            (
                outer_data,
                inner_data,
                join_column_outer,
                join_column_inner,
                true,
            )
        } else {
            (
                inner_data,
                outer_data,
                join_column_inner,
                join_column_outer,
                false,
            )
        };

    let mut hash_table: HashMap<&str, Vec<usize>> = HashMap::new();
    for (idx, row) in build_data.iter().enumerate() {
        if let Some(key) = row.get(build_col) {
            hash_table.entry(key.as_str()).or_default().push(idx);
        }
    }

    let mut result: Vec<JoinResultRow> = Vec::with_capacity(probe_data.len());
    let needs_build_tracking = *join_type == RedisJoinType::Left && build_is_outer;
    let mut matched_build: Vec<bool> = if needs_build_tracking {
        vec![false; build_data.len()]
    } else {
        Vec::new()
    };

    for (probe_idx, probe_row) in probe_data.iter().enumerate() {
        let mut matched = false;
        if let Some(probe_key) = probe_row.get(probe_col) {
            if let Some(build_indices) = hash_table.get(probe_key.as_str()) {
                matched = true;
                for &build_idx in build_indices {
                    if needs_build_tracking {
                        matched_build[build_idx] = true;
                    }
                    let row = if build_is_outer {
                        JoinResultRow::Matched {
                            outer_idx: build_idx,
                            inner_idx: probe_idx,
                        }
                    } else {
                        JoinResultRow::Matched {
                            outer_idx: probe_idx,
                            inner_idx: build_idx,
                        }
                    };
                    result.push(row);
                }
            }
        }
        if !matched && *join_type == RedisJoinType::Left && !build_is_outer {
            result.push(JoinResultRow::OuterOnly {
                outer_idx: probe_idx,
            });
        }
    }

    if needs_build_tracking {
        for (idx, matched) in matched_build.iter().enumerate() {
            if !matched {
                result.push(JoinResultRow::OuterOnly { outer_idx: idx });
            }
        }
    }

    result
}

fn fetch_dataset(
    conn: &mut dyn redis::ConnectionLike,
    table_type: &RedisTableType,
    key_prefix: &str,
) -> Vec<JoinRow> {
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
            pairs.into_iter().map(|(f, v)| smallvec![f, v]).collect()
        }
        RedisTableType::Set(_) => {
            let members: Vec<String> = redis::cmd("SMEMBERS")
                .arg(key_prefix)
                .query(conn)
                .unwrap_or_else(|e| {
                    pgrx::error!("Redis FDW: SMEMBERS '{}' failed: {}", key_prefix, e);
                });
            members.into_iter().map(|m| smallvec![m]).collect()
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
                .map(|(member, score)| smallvec![member, score.to_string()])
                .collect()
        }
        RedisTableType::List(list) => {
            let items: Vec<String> = redis::cmd("LRANGE")
                .arg(key_prefix)
                .arg(0i64)
                .arg(-1i64)
                .query(conn)
                .unwrap_or_else(|e| {
                    pgrx::error!("Redis FDW: LRANGE '{}' failed: {}", key_prefix, e);
                });
            if list.include_index {
                items
                    .into_iter()
                    .enumerate()
                    .map(|(idx, v)| smallvec![v, idx.to_string()])
                    .collect()
            } else {
                items.into_iter().map(|v| smallvec![v]).collect()
            }
        }
        RedisTableType::String(_) => {
            let val: Option<String> = redis::cmd("GET")
                .arg(key_prefix)
                .query(conn)
                .unwrap_or_else(|e| {
                    pgrx::error!("Redis FDW: GET '{}' failed: {}", key_prefix, e);
                });
            match val {
                Some(v) => vec![smallvec![key_prefix.to_string(), v]],
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
        RedisTableType::List(list) => {
            if list.include_index {
                2
            } else {
                1
            }
        }
        RedisTableType::String(_) => 2,
        RedisTableType::Stream(_) => 3,
        RedisTableType::None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tables::implementations::hash::RedisHashTable;
    use crate::tables::implementations::set::RedisSetTable;

    fn make_hash_type() -> RedisTableType {
        RedisTableType::Hash(RedisHashTable::new())
    }

    fn make_set_type() -> RedisTableType {
        RedisTableType::Set(RedisSetTable::new())
    }

    #[test]
    fn test_expected_columns_for_type() {
        assert_eq!(expected_columns_for_type(&make_hash_type()), 2);
        assert_eq!(expected_columns_for_type(&make_set_type()), 1);
        assert_eq!(expected_columns_for_type(&RedisTableType::None), 0);
    }

    #[test]
    fn test_inner_join_basic() {
        let outer_data: Vec<JoinRow> = vec![
            smallvec!["a".to_string(), "val_a".to_string()],
            smallvec!["b".to_string(), "val_b".to_string()],
            smallvec!["c".to_string(), "val_c".to_string()],
        ];
        let inner_data: Vec<JoinRow> = vec![
            smallvec!["a".to_string(), "inner_a".to_string()],
            smallvec!["b".to_string(), "inner_b".to_string()],
            smallvec!["d".to_string(), "inner_d".to_string()],
        ];

        let result = perform_hash_join(&outer_data, &inner_data, 0, 0, &RedisJoinType::Inner);
        assert_eq!(result.len(), 2);

        for row in &result {
            match row {
                JoinResultRow::Matched {
                    outer_idx,
                    inner_idx,
                } => {
                    assert_eq!(outer_data[*outer_idx][0], inner_data[*inner_idx][0]);
                }
                JoinResultRow::OuterOnly { .. } => {
                    panic!("INNER JOIN should not produce OuterOnly rows");
                }
            }
        }
    }

    #[test]
    fn test_left_join_with_nulls() {
        let outer_data: Vec<JoinRow> = vec![
            smallvec!["a".to_string(), "val_a".to_string()],
            smallvec!["b".to_string(), "val_b".to_string()],
            smallvec!["missing".to_string(), "val_m".to_string()],
        ];
        let inner_data: Vec<JoinRow> = vec![
            smallvec!["a".to_string(), "inner_a".to_string()],
            smallvec!["b".to_string(), "inner_b".to_string()],
        ];

        let result = perform_hash_join(&outer_data, &inner_data, 0, 0, &RedisJoinType::Left);
        assert_eq!(result.len(), 3);

        let outer_only_count = result
            .iter()
            .filter(|r| matches!(r, JoinResultRow::OuterOnly { .. }))
            .count();
        assert_eq!(outer_only_count, 1);

        for row in &result {
            if let JoinResultRow::OuterOnly { outer_idx } = row {
                assert_eq!(outer_data[*outer_idx][0], "missing");
            }
        }
    }

    #[test]
    fn test_inner_join_empty_outer() {
        let outer_data: Vec<JoinRow> = vec![];
        let inner_data: Vec<JoinRow> = vec![smallvec!["a".to_string(), "val".to_string()]];
        let result = perform_hash_join(&outer_data, &inner_data, 0, 0, &RedisJoinType::Inner);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_inner_join_empty_inner() {
        let outer_data: Vec<JoinRow> = vec![smallvec!["a".to_string(), "val".to_string()]];
        let inner_data: Vec<JoinRow> = vec![];
        let result = perform_hash_join(&outer_data, &inner_data, 0, 0, &RedisJoinType::Inner);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_left_join_empty_inner() {
        let outer_data: Vec<JoinRow> = vec![
            smallvec!["a".to_string(), "val_a".to_string()],
            smallvec!["b".to_string(), "val_b".to_string()],
        ];
        let inner_data: Vec<JoinRow> = vec![];
        let result = perform_hash_join(&outer_data, &inner_data, 0, 0, &RedisJoinType::Left);
        assert_eq!(result.len(), 2);
        assert!(result
            .iter()
            .all(|r| matches!(r, JoinResultRow::OuterOnly { .. })));
    }

    #[test]
    fn test_join_duplicate_keys() {
        let outer_data: Vec<JoinRow> = vec![
            smallvec!["dup".to_string()],
            smallvec!["dup".to_string()],
            smallvec!["unique".to_string()],
        ];
        let inner_data: Vec<JoinRow> = vec![
            smallvec!["dup".to_string(), "val_dup".to_string()],
            smallvec!["unique".to_string(), "val_unique".to_string()],
        ];
        let result = perform_hash_join(&outer_data, &inner_data, 0, 0, &RedisJoinType::Inner);
        assert_eq!(
            result.len(),
            3,
            "Duplicate keys should produce cross-product"
        );
    }

    #[test]
    fn test_left_join_outer_larger() {
        let outer_data: Vec<JoinRow> = vec![
            smallvec!["a".to_string(), "val_a".to_string()],
            smallvec!["b".to_string(), "val_b".to_string()],
            smallvec!["c".to_string(), "val_c".to_string()],
            smallvec!["d".to_string(), "val_d".to_string()],
            smallvec!["e".to_string(), "val_e".to_string()],
        ];
        let inner_data: Vec<JoinRow> = vec![smallvec!["a".to_string()], smallvec!["c".to_string()]];
        let result = perform_hash_join(&outer_data, &inner_data, 0, 0, &RedisJoinType::Left);
        assert_eq!(result.len(), 5, "LEFT JOIN should preserve all outer rows");

        let matched_count = result
            .iter()
            .filter(|r| matches!(r, JoinResultRow::Matched { .. }))
            .count();
        let outer_only_count = result
            .iter()
            .filter(|r| matches!(r, JoinResultRow::OuterOnly { .. }))
            .count();
        assert_eq!(matched_count, 2);
        assert_eq!(outer_only_count, 3);
    }
}
