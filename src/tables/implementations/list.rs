use crate::{
    query::{
        limit::LimitOffsetInfo,
        pushdown_types::{ComparisonOperator, PushableCondition},
        scan_ops::{extract_scan_conditions, PatternMatcher, ScanConditions},
    },
    tables::{
        interface::RedisTableOperations,
        types::{DataContainer, DataSet, LoadDataResult},
    },
};
use std::borrow::Cow;

/// Redis List table type
#[derive(Debug, Clone, Default)]
pub struct RedisListTable {
    pub dataset: DataSet,
    pub include_index: bool,
}

impl RedisListTable {
    pub fn new() -> Self {
        Self {
            dataset: DataSet::Empty,
            include_index: false,
        }
    }

    fn load_with_pattern_optimization(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        scan_conditions: &ScanConditions,
        limit_offset: &LimitOffsetInfo,
    ) -> Result<LoadDataResult, redis::RedisError> {
        // Load all list data first since Redis doesn't have LSCAN
        let all_data: Vec<String> = redis::cmd("LRANGE")
            .arg(key_prefix)
            .arg(0)
            .arg(-1)
            .query(conn)?;

        // Apply pattern filtering on the client side
        let mut filtered_data = Vec::new();

        for item in all_data {
            let mut matches = true;

            // Check pattern conditions
            for condition in &scan_conditions.pattern_conditions {
                match &condition.operator {
                    ComparisonOperator::Like => {
                        if let Some(matcher) = &scan_conditions.pattern_matcher {
                            if !matcher.matches(&item) {
                                matches = false;
                                break;
                            }
                        }
                    }
                    _ => {
                        matches = false;
                        break;
                    }
                }
            }

            // Check exact conditions
            for condition in &scan_conditions.exact_conditions {
                match &condition.operator {
                    ComparisonOperator::Equal => {
                        if item != condition.value {
                            matches = false;
                            break;
                        }
                    }
                    _ => {
                        matches = false;
                        break;
                    }
                }
            }

            if matches {
                filtered_data.push(item);
            }
        }

        // Apply LIMIT/OFFSET to filtered results
        if limit_offset.has_constraints() {
            filtered_data = limit_offset.apply_to_vec(filtered_data);
        }

        if filtered_data.is_empty() {
            self.dataset = DataSet::Empty;
            Ok(LoadDataResult::Empty)
        } else {
            self.dataset = DataSet::Filtered(filtered_data);
            Ok(LoadDataResult::FullyLoaded)
        }
    }
}

impl RedisTableOperations for RedisListTable {
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
        limit_offset: &LimitOffsetInfo,
    ) -> Result<LoadDataResult, redis::RedisError> {
        if let Some(conditions) = conditions {
            let scan_conditions = extract_scan_conditions(conditions);

            // Check for pattern-optimizable conditions
            if scan_conditions.has_optimizable_conditions() {
                return self.load_with_pattern_optimization(
                    conn,
                    key_prefix,
                    &scan_conditions,
                    limit_offset,
                );
            }

            // Handle simple Equal conditions efficiently
            if !conditions.is_empty() {
                for condition in conditions {
                    if let ComparisonOperator::Equal = condition.operator {
                        // For lists, we need to load all data and filter
                        let all_data: Vec<String> = if limit_offset.has_constraints() {
                            // Apply LIMIT/OFFSET directly with LRANGE for better performance
                            let offset = limit_offset.offset.unwrap_or(0);
                            let limit = limit_offset.limit.unwrap_or(usize::MAX);
                            let start = offset as isize;
                            let end = if limit == usize::MAX {
                                -1isize
                            } else {
                                (offset + limit - 1) as isize
                            };
                            redis::cmd("LRANGE")
                                .arg(key_prefix)
                                .arg(start)
                                .arg(end)
                                .query(conn)?
                        } else {
                            redis::cmd("LRANGE")
                                .arg(key_prefix)
                                .arg(0)
                                .arg(-1)
                                .query(conn)?
                        };

                        let filtered: Vec<String> = all_data
                            .into_iter()
                            .filter(|item| item == &condition.value)
                            .collect();

                        return if filtered.is_empty() {
                            self.dataset = DataSet::Empty;
                            Ok(LoadDataResult::Empty)
                        } else {
                            self.dataset = DataSet::Filtered(filtered);
                            Ok(LoadDataResult::FullyLoaded)
                        };
                    }
                }
            }
        }

        // Lists don't have efficient filtering in Redis
        // Fall back to loading all data, but apply LIMIT/OFFSET directly with LRANGE
        let (start, end) = if limit_offset.has_constraints() {
            let offset = limit_offset.offset.unwrap_or(0);
            let limit = limit_offset.limit.unwrap_or(usize::MAX);
            let start = offset as isize;
            let end = if limit == usize::MAX {
                -1isize // Redis LRANGE end=-1 means to the end of list
            } else {
                (offset + limit - 1) as isize
            };
            (start, end)
        } else {
            (0, -1) // Get all elements
        };

        let data: Vec<String> = redis::cmd("LRANGE")
            .arg(key_prefix)
            .arg(start)
            .arg(end)
            .query(conn)?;
        self.dataset = DataSet::Complete(DataContainer::List(data));
        Ok(LoadDataResult::FullyLoaded)
    }

    fn get_dataset(&self) -> &DataSet {
        &self.dataset
    }

    fn get_row(&self, index: usize) -> Option<Vec<Cow<'_, str>>> {
        if self.include_index {
            match &self.dataset {
                DataSet::Complete(DataContainer::List(items)) => items
                    .get(index)
                    .map(|item| vec![Cow::Owned(index.to_string()), Cow::Borrowed(item.as_str())]),
                DataSet::Filtered(items) => items
                    .get(index)
                    .map(|item| vec![Cow::Owned(index.to_string()), Cow::Borrowed(item.as_str())]),
                _ => None,
            }
        } else {
            self.dataset.get_row(index)
        }
    }

    fn insert(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        if !data.is_empty() {
            // Single RPUSH with all values (RPUSH supports multiple values)
            let _: i32 = redis::cmd("RPUSH").arg(key_prefix).arg(data).query(conn)?;
        }
        Ok(())
    }

    fn delete(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        if !data.is_empty() {
            // LREM requires separate calls per value (different values to remove)
            // Try pipeline first (single round-trip), fall back to individual commands for cluster
            let pipe_result: Result<Vec<i32>, _> = {
                let mut pipe = redis::pipe();
                for value in data {
                    pipe.cmd("LREM").arg(key_prefix).arg(0).arg(value);
                }
                pipe.query(conn)
            };

            if pipe_result.is_err() {
                // Fallback: individual LREM commands (cluster mode)
                for value in data {
                    let _: i32 = redis::cmd("LREM")
                        .arg(key_prefix)
                        .arg(0)
                        .arg(value)
                        .query(conn)?;
                }
            }
        }
        Ok(())
    }

    fn update(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        old_data: &[String],
        new_data: &[String],
    ) -> Result<(), redis::RedisError> {
        // Find the position of old value, then LSET at that index
        if let (Some(old_value), Some(new_value)) = (old_data.first(), new_data.first()) {
            if old_value == new_value {
                return Ok(());
            }
            let positions: Vec<i64> = redis::cmd("LPOS")
                .arg(key_prefix)
                .arg(old_value)
                .arg("COUNT")
                .arg(1)
                .query(conn)?;

            if let Some(&idx) = positions.first() {
                let _: () = redis::cmd("LSET")
                    .arg(key_prefix)
                    .arg(idx)
                    .arg(new_value)
                    .query(conn)?;
            }
        }
        Ok(())
    }

    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        matches!(
            operator,
            ComparisonOperator::Equal | ComparisonOperator::Like | ComparisonOperator::In
        )
    }

    fn load_batch(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        cursor: u64,
        batch_size: usize,
        conditions: Option<&[PushableCondition]>,
    ) -> Result<(u64, usize), redis::RedisError> {
        // Lists use offset-based pagination (cursor = offset index)
        let start = cursor as isize;
        let end = start + (batch_size as isize) - 1;
        let data: Vec<String> = redis::cmd("LRANGE")
            .arg(key_prefix)
            .arg(start)
            .arg(end)
            .query(conn)?;
        let row_count = data.len();
        let new_cursor = if row_count < batch_size {
            0 // no more data
        } else {
            cursor + row_count as u64
        };

        // Apply conditions as client-side post-filter (no LSCAN in Redis)
        let filtered: Vec<String> = if let Some(conds) = conditions {
            let like_matchers: Vec<(usize, PatternMatcher)> = conds
                .iter()
                .enumerate()
                .filter(|(_, c)| c.operator == ComparisonOperator::Like)
                .map(|(i, c)| (i, PatternMatcher::from_like_pattern(&c.value)))
                .collect();
            data.into_iter()
                .filter(|item| {
                    conds.iter().enumerate().all(|(i, c)| match c.operator {
                        ComparisonOperator::Equal => item == &c.value,
                        ComparisonOperator::NotEqual => item != &c.value,
                        ComparisonOperator::Like => like_matchers
                            .iter()
                            .find(|(idx, _)| *idx == i)
                            .is_some_and(|(_, m)| m.matches(item)),
                        ComparisonOperator::In => c.value.split(',').any(|s| s == item),
                        _ => true,
                    })
                })
                .collect()
        } else {
            data
        };

        let filtered_count = filtered.len();
        self.dataset = if filtered.is_empty() {
            DataSet::Empty
        } else {
            DataSet::Complete(DataContainer::List(filtered))
        };
        Ok((new_cursor, filtered_count))
    }

    fn configure(
        &mut self,
        column_names: &[String],
        _pushdown_column_index: usize,
        _score_column_index: Option<usize>,
    ) {
        self.include_index = column_names.len() >= 2;
    }

    fn load_multi_key_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        keys: &[String],
    ) -> Result<Vec<String>, redis::RedisError> {
        const PER_KEY_WARN_THRESHOLD: usize = 200_000;
        let mut pipe = redis::pipe();
        for key in keys {
            pipe.cmd("LRANGE").arg(key).arg(0i64).arg(-1i64);
        }
        let results: Vec<Vec<String>> = pipe.query(conn)?;
        let mut all_rows = Vec::with_capacity(keys.len() * self.multi_key_columns_per_row());
        for (key, items) in keys.iter().zip(results) {
            pgrx::check_for_interrupts!();
            if items.len() > PER_KEY_WARN_THRESHOLD {
                pgrx::warning!(
                    "Redis FDW: key '{}' contains {} elements, consider using LIMIT",
                    key,
                    items.len()
                );
            }
            for item in items {
                all_rows.push(key.clone());
                all_rows.push(item);
            }
        }
        Ok(all_rows)
    }

    fn clear(&mut self) {
        self.dataset = DataSet::default();
    }

    fn redis_type_name(&self) -> &'static str {
        "list"
    }

    fn set_filtered_data(&mut self, data: Vec<String>) {
        self.dataset = DataSet::Filtered(data);
    }
}
