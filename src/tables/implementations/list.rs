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
        _limit_offset: &LimitOffsetInfo,
    ) -> Result<LoadDataResult, redis::RedisError> {
        if let Some(conditions) = conditions {
            let scan_conditions = extract_scan_conditions(conditions);

            // Check for pattern-optimizable conditions
            if scan_conditions.has_optimizable_conditions() {
                return self.load_with_pattern_optimization(conn, key_prefix, &scan_conditions);
            }

            // Handle simple Equal conditions efficiently
            if !conditions.is_empty() {
                for condition in conditions {
                    if let ComparisonOperator::Equal = condition.operator {
                        // For lists, we need to load all data and filter
                        let all_data: Vec<String> = redis::cmd("LRANGE")
                            .arg(key_prefix)
                            .arg(0)
                            .arg(-1)
                            .query(conn)?;

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
        // Fall back to loading all data
        let data: Vec<String> = redis::cmd("LRANGE")
            .arg(key_prefix)
            .arg(0)
            .arg(-1)
            .query(conn)?;
        self.dataset = DataSet::Complete(DataContainer::List(data));
        Ok(LoadDataResult::LoadedToInternal)
    }

    fn get_dataset(&self) -> &DataSet {
        &self.dataset
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

    fn set_filtered_data(&mut self, data: Vec<String>) {
        self.dataset = DataSet::Filtered(data);
    }
}
