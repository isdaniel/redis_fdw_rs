use crate::{
    query::{
        limit::LimitOffsetInfo,
        pushdown_types::{ComparisonOperator, PushableCondition},
        scan_ops::{extract_scan_conditions, ScanConditions},
    },
    tables::{
        interface::RedisTableOperations,
        types::{DataContainer, DataSet, LoadDataResult},
    },
};

/// Redis List table type
#[derive(Debug, Clone, Default)]
pub struct RedisListTable {
    pub dataset: DataSet,
}

impl RedisListTable {
    pub fn new() -> Self {
        Self {
            dataset: DataSet::Empty,
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
            self.dataset = DataSet::Filtered(filtered_data.clone());
            Ok(LoadDataResult::PushdownApplied(filtered_data))
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
                            self.dataset = DataSet::Filtered(filtered.clone());
                            Ok(LoadDataResult::PushdownApplied(filtered))
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

    fn get_dataset_mut(&mut self) -> &mut DataSet {
        &mut self.dataset
    }

    fn insert(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        for value in data {
            let _: i32 = redis::cmd("RPUSH").arg(key_prefix).arg(value).query(conn)?;
        }
        Ok(())
    }

    fn delete(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        for value in data {
            // LREM removes all occurrences of value from the list
            // Using count = 0 to remove all occurrences
            let _: i32 = redis::cmd("LREM")
                .arg(key_prefix)
                .arg(0)
                .arg(value)
                .query(conn)?;
        }
        Ok(())
    }

    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        matches!(
            operator,
            ComparisonOperator::Equal | ComparisonOperator::Like
        )
    }
}
