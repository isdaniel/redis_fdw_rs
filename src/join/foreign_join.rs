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

    let result = perform_hash_join(
        &outer_data,
        &inner_data,
        state.join_column_outer,
        state.join_column_inner,
        &state.join_type,
        &state.inner_table_type,
    );

    let count = result.len();
    state.result_columns = expected_columns_for_type(&state.outer_table_type)
        + expected_columns_for_type(&state.inner_table_type);
    state.result_data = result;
    state.current_row = 0;
    count
}

pub(crate) fn perform_hash_join(
    outer_data: &[Vec<String>],
    inner_data: &[Vec<String>],
    join_column_outer: usize,
    join_column_inner: usize,
    join_type: &RedisJoinType,
    inner_table_type: &RedisTableType,
) -> Vec<Vec<Option<String>>> {
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

    let inner_cols = expected_columns_for_type(inner_table_type);

    let mut result: Vec<Vec<Option<String>>> = Vec::new();
    let needs_build_tracking = *join_type == RedisJoinType::Left && build_is_outer;
    let mut matched_build: Vec<bool> = if needs_build_tracking {
        vec![false; build_data.len()]
    } else {
        Vec::new()
    };

    let null_pad_row: Vec<Option<String>> = if *join_type == RedisJoinType::Left && !build_is_outer
    {
        vec![None; inner_cols]
    } else {
        Vec::new()
    };

    for probe_row in probe_data {
        if let Some(probe_key) = probe_row.get(probe_col) {
            if let Some(build_indices) = hash_table.get(probe_key.as_str()) {
                for &build_idx in build_indices {
                    if needs_build_tracking {
                        matched_build[build_idx] = true;
                    }
                    let combined = if build_is_outer {
                        to_option_row(&build_data[build_idx], probe_row)
                    } else {
                        to_option_row(probe_row, &build_data[build_idx])
                    };
                    result.push(combined);
                }
            } else if *join_type == RedisJoinType::Left && !build_is_outer {
                let mut combined = to_some_vec(probe_row);
                combined.extend_from_slice(&null_pad_row);
                result.push(combined);
            }
        } else if *join_type == RedisJoinType::Left && !build_is_outer {
            let mut combined = to_some_vec(probe_row);
            combined.extend_from_slice(&null_pad_row);
            result.push(combined);
        }
    }

    if needs_build_tracking {
        let null_inner: Vec<Option<String>> = vec![None; inner_cols];
        for (idx, matched) in matched_build.iter().enumerate() {
            if !matched {
                let mut combined = to_some_vec(&build_data[idx]);
                combined.extend_from_slice(&null_inner);
                result.push(combined);
            }
        }
    }

    result
}

fn fetch_dataset(
    conn: &mut dyn redis::ConnectionLike,
    table_type: &RedisTableType,
    key_prefix: &str,
) -> Vec<Vec<String>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tables::implementations::hash::RedisHashTable;
    use crate::tables::implementations::list::RedisListTable;
    use crate::tables::implementations::set::RedisSetTable;
    use crate::tables::implementations::zset::RedisZSetTable;

    fn make_hash_type() -> RedisTableType {
        RedisTableType::Hash(RedisHashTable::new())
    }

    fn make_set_type() -> RedisTableType {
        RedisTableType::Set(RedisSetTable::new())
    }

    fn make_zset_type() -> RedisTableType {
        RedisTableType::ZSet(RedisZSetTable::new())
    }

    fn make_list_type() -> RedisTableType {
        RedisTableType::List(RedisListTable::new())
    }

    #[test]
    fn test_expected_columns_for_type() {
        assert_eq!(expected_columns_for_type(&make_hash_type()), 2);
        assert_eq!(expected_columns_for_type(&make_set_type()), 1);
        assert_eq!(expected_columns_for_type(&make_zset_type()), 2);
        assert_eq!(expected_columns_for_type(&make_list_type()), 1);
        assert_eq!(expected_columns_for_type(&RedisTableType::None), 0);
    }

    #[test]
    fn test_to_option_row() {
        let outer = vec!["a".to_string(), "b".to_string()];
        let inner = vec!["c".to_string()];
        let result = to_option_row(&outer, &inner);
        assert_eq!(
            result,
            vec![
                Some("a".to_string()),
                Some("b".to_string()),
                Some("c".to_string()),
            ]
        );
    }

    #[test]
    fn test_to_option_row_empty_inner() {
        let outer = vec!["x".to_string()];
        let inner: Vec<String> = vec![];
        let result = to_option_row(&outer, &inner);
        assert_eq!(result, vec![Some("x".to_string())]);
    }

    #[test]
    fn test_to_some_vec() {
        let data = vec!["a".to_string(), "b".to_string()];
        let result = to_some_vec(&data);
        assert_eq!(result, vec![Some("a".to_string()), Some("b".to_string())]);
    }

    #[test]
    fn test_to_some_vec_empty() {
        let data: Vec<String> = vec![];
        let result = to_some_vec(&data);
        assert!(result.is_empty());
    }

    #[test]
    fn test_execute_inner_join_basic() {
        let outer_data = vec![
            vec!["a".to_string(), "val_a".to_string()],
            vec!["b".to_string(), "val_b".to_string()],
            vec!["c".to_string(), "val_c".to_string()],
        ];
        let inner_data = vec![
            vec!["a".to_string(), "inner_a".to_string()],
            vec!["b".to_string(), "inner_b".to_string()],
            vec!["d".to_string(), "inner_d".to_string()],
        ];

        let result = perform_hash_join(
            &outer_data,
            &inner_data,
            0,
            0,
            &RedisJoinType::Inner,
            &make_hash_type(),
        );
        assert_eq!(result.len(), 2);

        let has_a = result
            .iter()
            .any(|r| r[0] == Some("a".to_string()) && r[2] == Some("a".to_string()));
        let has_b = result
            .iter()
            .any(|r| r[0] == Some("b".to_string()) && r[2] == Some("b".to_string()));
        assert!(has_a, "Should contain row for key 'a'");
        assert!(has_b, "Should contain row for key 'b'");
    }

    #[test]
    fn test_execute_left_join_with_nulls() {
        let outer_data = vec![
            vec!["a".to_string(), "val_a".to_string()],
            vec!["b".to_string(), "val_b".to_string()],
            vec!["missing".to_string(), "val_m".to_string()],
        ];
        let inner_data = vec![
            vec!["a".to_string(), "inner_a".to_string()],
            vec!["b".to_string(), "inner_b".to_string()],
        ];

        let result = perform_hash_join(
            &outer_data,
            &inner_data,
            0,
            0,
            &RedisJoinType::Left,
            &make_hash_type(),
        );
        assert_eq!(result.len(), 3);

        let null_row = result.iter().find(|r| r[0] == Some("missing".to_string()));
        assert!(null_row.is_some(), "Should have row for 'missing'");
        let nr = null_row.unwrap();
        assert_eq!(nr[2], None, "Inner field should be NULL");
        assert_eq!(nr[3], None, "Inner value should be NULL");
    }

    #[test]
    fn test_execute_inner_join_empty_outer() {
        let outer_data: Vec<Vec<String>> = vec![];
        let inner_data = vec![vec!["a".to_string(), "val".to_string()]];

        let result = perform_hash_join(
            &outer_data,
            &inner_data,
            0,
            0,
            &RedisJoinType::Inner,
            &make_hash_type(),
        );
        assert_eq!(result.len(), 0, "Empty outer should produce 0 results");
    }

    #[test]
    fn test_execute_inner_join_empty_inner() {
        let outer_data = vec![vec!["a".to_string(), "val".to_string()]];
        let inner_data: Vec<Vec<String>> = vec![];

        let result = perform_hash_join(
            &outer_data,
            &inner_data,
            0,
            0,
            &RedisJoinType::Inner,
            &make_hash_type(),
        );
        assert_eq!(
            result.len(),
            0,
            "Empty inner should produce 0 results for INNER JOIN"
        );
    }

    #[test]
    fn test_execute_left_join_empty_inner() {
        let outer_data = vec![
            vec!["a".to_string(), "val_a".to_string()],
            vec!["b".to_string(), "val_b".to_string()],
        ];
        let inner_data: Vec<Vec<String>> = vec![];

        let result = perform_hash_join(
            &outer_data,
            &inner_data,
            0,
            0,
            &RedisJoinType::Left,
            &make_hash_type(),
        );
        assert_eq!(
            result.len(),
            2,
            "LEFT JOIN with empty inner should return all outer rows"
        );
        assert_eq!(result[0][2], None, "Inner columns should be NULL");
        assert_eq!(result[0][3], None, "Inner columns should be NULL");
    }

    #[test]
    fn test_execute_join_build_probe_strategy() {
        let outer_data = vec![
            vec!["a".to_string(), "o_a".to_string()],
            vec!["b".to_string(), "o_b".to_string()],
        ];
        let inner_data = vec![
            vec!["a".to_string(), "i_a".to_string()],
            vec!["b".to_string(), "i_b".to_string()],
            vec!["c".to_string(), "i_c".to_string()],
            vec!["d".to_string(), "i_d".to_string()],
            vec!["e".to_string(), "i_e".to_string()],
        ];

        let result = perform_hash_join(
            &outer_data,
            &inner_data,
            0,
            0,
            &RedisJoinType::Inner,
            &make_hash_type(),
        );
        assert_eq!(result.len(), 2, "Should match 'a' and 'b'");

        let row_a = result
            .iter()
            .find(|r| r[0] == Some("a".to_string()))
            .unwrap();
        assert_eq!(
            row_a[1],
            Some("o_a".to_string()),
            "Outer value should be in position 1"
        );
        assert_eq!(
            row_a[2],
            Some("a".to_string()),
            "Inner field should be in position 2"
        );
        assert_eq!(
            row_a[3],
            Some("i_a".to_string()),
            "Inner value should be in position 3"
        );
    }

    #[test]
    fn test_execute_join_duplicate_keys_in_build() {
        let outer_data = vec![
            vec!["dup".to_string()],
            vec!["dup".to_string()],
            vec!["unique".to_string()],
        ];
        let inner_data = vec![
            vec!["dup".to_string(), "val_dup".to_string()],
            vec!["unique".to_string(), "val_unique".to_string()],
        ];

        let result = perform_hash_join(
            &outer_data,
            &inner_data,
            0,
            0,
            &RedisJoinType::Inner,
            &make_hash_type(),
        );
        assert_eq!(
            result.len(),
            3,
            "Duplicate keys should produce cross-product"
        );
    }

    #[test]
    fn test_execute_left_join_outer_larger_than_inner() {
        let outer_data = vec![
            vec!["a".to_string(), "val_a".to_string()],
            vec!["b".to_string(), "val_b".to_string()],
            vec!["c".to_string(), "val_c".to_string()],
            vec!["d".to_string(), "val_d".to_string()],
            vec!["e".to_string(), "val_e".to_string()],
        ];
        let inner_data = vec![vec!["a".to_string()], vec!["c".to_string()]];

        let result = perform_hash_join(
            &outer_data,
            &inner_data,
            0,
            0,
            &RedisJoinType::Left,
            &make_set_type(),
        );
        assert_eq!(result.len(), 5, "LEFT JOIN should preserve all outer rows");

        let row_a = result
            .iter()
            .find(|r| r[0] == Some("a".to_string()))
            .unwrap();
        assert_eq!(
            row_a[2],
            Some("a".to_string()),
            "Matched row should have inner value"
        );

        let row_b = result
            .iter()
            .find(|r| r[0] == Some("b".to_string()))
            .unwrap();
        assert_eq!(row_b[2], None, "Unmatched row should have NULL inner");
    }
}
