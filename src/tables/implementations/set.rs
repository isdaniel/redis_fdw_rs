use crate::{
    query::{
        pushdown_types::{ComparisonOperator, PushableCondition},
        scan_ops::{extract_scan_conditions, PatternMatcher, RedisScanBuilder},
    },
    tables::{
        interface::RedisTableOperations,
        types::{DataContainer, DataSet, LoadDataResult},
    },
};

/// Redis Set table type
#[derive(Debug, Clone, Default)]
pub struct RedisSetTable {
    pub dataset: DataSet,
}

impl RedisSetTable {
    pub fn new() -> Self {
        Self {
            dataset: DataSet::Empty,
        }
    }

    /// Load data with SSCAN optimization for pattern matching
    fn load_with_scan_optimization(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        scan_conditions: &crate::query::scan_ops::ScanConditions,
    ) -> Result<LoadDataResult, redis::RedisError> {
        if let Some(pattern) = scan_conditions.get_primary_pattern() {
            let pattern_matcher = PatternMatcher::from_like_pattern(&pattern);

            if pattern_matcher.requires_scan() {
                // Use SSCAN with MATCH to find matching members
                let matching_members: Vec<String> = RedisScanBuilder::new_set_scan(key_prefix)
                    .with_pattern(pattern_matcher.get_pattern())
                    .with_count(100)
                    .execute_all(conn)?;

                // Additional client-side filtering
                let filtered_members: Vec<String> = matching_members
                    .into_iter()
                    .filter(|member| {
                        scan_conditions.pattern_conditions.iter().all(|condition| {
                            let matcher = PatternMatcher::from_like_pattern(&condition.value);
                            matcher.matches(member)
                        })
                    })
                    .collect();

                if filtered_members.is_empty() {
                    self.dataset = DataSet::Empty;
                    Ok(LoadDataResult::Empty)
                } else {
                    self.dataset = DataSet::Filtered(filtered_members.clone());
                    Ok(LoadDataResult::PushdownApplied(filtered_members))
                }
            } else {
                // Exact member match
                let exists: bool = redis::cmd("SISMEMBER")
                    .arg(key_prefix)
                    .arg(&pattern)
                    .query(conn)?;

                if exists {
                    let result = vec![pattern.clone()];
                    self.dataset = DataSet::Filtered(result.clone());
                    Ok(LoadDataResult::PushdownApplied(result))
                } else {
                    self.dataset = DataSet::Empty;
                    Ok(LoadDataResult::Empty)
                }
            }
        } else {
            // No pattern available, fallback to regular load
            let members: Vec<String> = redis::cmd("SMEMBERS").arg(key_prefix).query(conn)?;
            self.dataset = DataSet::Complete(DataContainer::Set(members));
            Ok(LoadDataResult::LoadedToInternal)
        }
    }
}

impl RedisTableOperations for RedisSetTable {
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
    ) -> Result<LoadDataResult, redis::RedisError> {
        if let Some(conditions) = conditions {
            let scan_conditions = extract_scan_conditions(conditions);

            // Check for SCAN-optimizable conditions first
            if scan_conditions.has_optimizable_conditions() {
                return self.load_with_scan_optimization(conn, key_prefix, &scan_conditions);
            }

            // Legacy optimization for non-pattern conditions
            if !conditions.is_empty() {
                for condition in conditions {
                    match condition.operator {
                        ComparisonOperator::Equal => {
                            // SISMEMBER for specific member
                            let exists: bool = redis::cmd("SISMEMBER")
                                .arg(key_prefix)
                                .arg(&condition.value)
                                .query(conn)?;

                            return if exists {
                                let filtered_data = vec![condition.value.clone()];
                                self.dataset = DataSet::Filtered(filtered_data.clone());
                                Ok(LoadDataResult::PushdownApplied(filtered_data))
                            } else {
                                self.dataset = DataSet::Empty;
                                Ok(LoadDataResult::Empty)
                            };
                        }
                        ComparisonOperator::In => {
                            // Check multiple members
                            let members: Vec<&str> = condition.value.split(',').collect();
                            let mut result = Vec::new();

                            for member in members {
                                let exists: bool = redis::cmd("SISMEMBER")
                                    .arg(key_prefix)
                                    .arg(member)
                                    .query(conn)?;

                                if exists {
                                    result.push(member.to_string());
                                }
                            }
                            self.dataset = DataSet::Filtered(result.clone());
                            return Ok(LoadDataResult::PushdownApplied(result));
                        }
                        _ => {} // Fall back to full scan
                    }
                }
            }
        }

        // Load all data into internal storage
        let data: Vec<String> = redis::cmd("SMEMBERS").arg(key_prefix).query(conn)?;
        self.dataset = DataSet::Complete(DataContainer::Set(data));
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
            let _added: i32 = redis::cmd("SADD").arg(key_prefix).arg(value).query(conn)?;
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
            let _: i32 = redis::cmd("SREM").arg(key_prefix).arg(value).query(conn)?;
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
        // For sets, update means remove old and add new
        self.delete(conn, key_prefix, old_data)?;
        self.insert(conn, key_prefix, new_data)?;
        Ok(())
    }

    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        matches!(
            operator,
            ComparisonOperator::Equal | ComparisonOperator::In | ComparisonOperator::Like
        )
    }
}
