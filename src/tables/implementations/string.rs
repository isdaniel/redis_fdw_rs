use crate::{
    query::{
        limit::LimitOffsetInfo,
        pushdown_types::{ComparisonOperator, PushableCondition},
        scan_ops::{extract_scan_conditions, PatternMatcher},
    },
    tables::{
        interface::RedisTableOperations,
        types::{DataContainer, DataSet, LoadDataResult},
    },
};

/// Redis String table type
#[derive(Debug, Clone, Default)]
pub struct RedisStringTable {
    pub dataset: DataSet,
}

impl RedisStringTable {
    pub fn new() -> Self {
        Self {
            dataset: DataSet::Empty,
        }
    }

    /// Load data with SCAN optimization for value matching
    fn load_with_scan_optimization(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        scan_conditions: &crate::query::scan_ops::ScanConditions,
        limit_offset: &LimitOffsetInfo,
    ) -> Result<LoadDataResult, redis::RedisError> {
        // For string tables, we need to check the stored value against conditions
        // Get the value from Redis
        let stored_value: Option<String> = redis::cmd("GET").arg(key_prefix).query(conn)?;

        if let Some(value) = stored_value {
            // Check if the value matches any of the conditions
            let mut matches = true;

            // Check exact match conditions
            for condition in &scan_conditions.exact_conditions {
                if condition.operator == ComparisonOperator::Equal {
                    if value != condition.value {
                        matches = false;
                        break;
                    }
                }
            }

            // Check pattern conditions
            if matches {
                for condition in &scan_conditions.pattern_conditions {
                    if condition.operator == ComparisonOperator::Like {
                        let pattern_matcher = PatternMatcher::from_like_pattern(&condition.value);
                        if !pattern_matcher.matches(&value) {
                            matches = false;
                            break;
                        }
                    }
                }
            }

            if matches {
                // Apply LIMIT/OFFSET constraints
                if let Some(offset) = limit_offset.offset {
                    if offset > 0 {
                        self.dataset = DataSet::Empty;
                        return Ok(LoadDataResult::Empty);
                    }
                }

                if let Some(limit) = limit_offset.limit {
                    if limit == 0 {
                        self.dataset = DataSet::Empty;
                        return Ok(LoadDataResult::Empty);
                    }
                }

                self.dataset = DataSet::Complete(DataContainer::String(Some(value)));
                Ok(LoadDataResult::PushdownApplied(
                    vec![key_prefix.to_string()],
                ))
            } else {
                self.dataset = DataSet::Empty;
                Ok(LoadDataResult::Empty)
            }
        } else {
            // Key doesn't exist
            self.dataset = DataSet::Empty;
            Ok(LoadDataResult::Empty)
        }
    }
}

impl RedisTableOperations for RedisStringTable {
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
        limit_offset: &LimitOffsetInfo,
    ) -> Result<LoadDataResult, redis::RedisError> {
        if let Some(conditions) = conditions {
            let scan_conditions = extract_scan_conditions(conditions);

            // For string tables, we can optimize by scanning keys with patterns
            if scan_conditions.has_optimizable_conditions() {
                return self.load_with_scan_optimization(
                    conn,
                    key_prefix,
                    &scan_conditions,
                    limit_offset,
                );
            }
        }

        // Fallback: Load single key without optimization
        let value: Option<String> = redis::cmd("GET").arg(key_prefix).query(conn)?;

        // Apply LIMIT/OFFSET constraints - for string tables, OFFSET > 0 means no results
        if let Some(offset) = limit_offset.offset {
            if offset > 0 {
                self.dataset = DataSet::Empty;
                return Ok(LoadDataResult::Empty);
            }
        }

        // Apply LIMIT - if LIMIT is 0, return empty
        if let Some(limit) = limit_offset.limit {
            if limit == 0 {
                self.dataset = DataSet::Empty;
                return Ok(LoadDataResult::Empty);
            }
        }

        self.dataset = DataSet::Complete(DataContainer::String(value));
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
        if let Some(value) = data.first() {
            let _: () = redis::cmd("SET").arg(key_prefix).arg(value).query(conn)?;
        }
        Ok(())
    }

    fn delete(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        _data: &[String],
    ) -> Result<(), redis::RedisError> {
        let _: () = redis::cmd("DEL").arg(key_prefix).query(conn)?;
        Ok(())
    }

    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        matches!(
            operator,
            ComparisonOperator::Equal | ComparisonOperator::Like
        )
    }
}
