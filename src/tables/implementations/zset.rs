use crate::{
    query::{
        limit::LimitOffsetInfo,
        pushdown_types::{ComparisonOperator, PushableCondition},
        scan_ops::{extract_scan_conditions, RedisScanBuilder, ScanConditions},
    },
    tables::{
        interface::RedisTableOperations,
        types::{DataContainer, DataSet, LoadDataResult},
    },
};

/// Redis Sorted Set table type
#[derive(Debug, Clone, Default)]
pub struct RedisZSetTable {
    pub dataset: DataSet,
}

impl RedisZSetTable {
    pub fn new() -> Self {
        Self {
            dataset: DataSet::Empty,
        }
    }

    fn load_with_scan_optimization(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        scan_conditions: &ScanConditions,
    ) -> Result<LoadDataResult, redis::RedisError> {
        let mut scan_builder = RedisScanBuilder::new_zset_scan(key_prefix);

        // Add pattern from the first LIKE condition
        if let Some(_pattern_condition) = scan_conditions.pattern_conditions.first() {
            if let Some(matcher) = &scan_conditions.pattern_matcher {
                scan_builder = scan_builder.with_pattern(matcher.get_pattern());
            }
        }

        // Execute ZSCAN to get member-score pairs
        let all_members: Vec<String> = scan_builder.execute_all(conn)?;

        let mut filtered_data = Vec::new();

        // ZSCAN returns member-score pairs, so we process them in chunks of 2
        for chunk in all_members.chunks(2) {
            if chunk.len() == 2 {
                let member = &chunk[0];
                let score = &chunk[1];

                // Apply additional client-side filtering if needed
                let mut matches = true;

                // Check pattern conditions
                for condition in &scan_conditions.pattern_conditions {
                    match &condition.operator {
                        ComparisonOperator::Like => {
                            if let Some(matcher) = &scan_conditions.pattern_matcher {
                                if !matcher.matches(member) {
                                    matches = false;
                                    break;
                                }
                            }
                        }
                        _ => {}
                    }
                }

                // Check exact conditions
                for condition in &scan_conditions.exact_conditions {
                    match &condition.operator {
                        ComparisonOperator::Equal => {
                            if member != &condition.value {
                                matches = false;
                                break;
                            }
                        }
                        _ => {}
                    }
                }

                if matches {
                    filtered_data.push(member.clone());
                    filtered_data.push(score.clone());
                }
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

impl RedisTableOperations for RedisZSetTable {
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
        _limit_offset: &LimitOffsetInfo,
    ) -> Result<LoadDataResult, redis::RedisError> {
        if let Some(conditions) = conditions {
            let scan_conditions = extract_scan_conditions(conditions);

            // Check for SCAN-optimizable conditions first
            if scan_conditions.has_optimizable_conditions() {
                return self.load_with_scan_optimization(conn, key_prefix, &scan_conditions);
            }

            // Legacy optimization for specific member lookups
            if !conditions.is_empty() {
                for condition in conditions {
                    match condition.operator {
                        ComparisonOperator::Equal => {
                            // Check if member exists and get its score
                            let score: Option<f64> = redis::cmd("ZSCORE")
                                .arg(key_prefix)
                                .arg(&condition.value)
                                .query(conn)?;

                            return if let Some(score) = score {
                                let filtered_data =
                                    vec![condition.value.clone(), score.to_string()];
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
                                let score: Option<f64> = redis::cmd("ZSCORE")
                                    .arg(key_prefix)
                                    .arg(member)
                                    .query(conn)?;

                                if let Some(score) = score {
                                    result.push(member.to_string());
                                    result.push(score.to_string());
                                }
                            }

                            return if result.is_empty() {
                                self.dataset = DataSet::Empty;
                                Ok(LoadDataResult::Empty)
                            } else {
                                self.dataset = DataSet::Filtered(result.clone());
                                Ok(LoadDataResult::PushdownApplied(result))
                            };
                        }
                        _ => {} // Fall back to full scan
                    }
                }
            }
        }

        // ZSets could support score-based range queries in the future
        // For now, fall back to loading all data
        let result: Vec<(String, f64)> = redis::cmd("ZRANGE")
            .arg(key_prefix)
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query(conn)?;
        self.dataset = DataSet::Complete(DataContainer::ZSet(result));
        Ok(LoadDataResult::LoadedToInternal)
    }

    fn get_dataset(&self) -> &DataSet {
        &self.dataset
    }

    /// Override the default get_row implementation to handle zset-specific filtered data format
    fn get_row(&self, index: usize) -> Option<Vec<String>> {
        match &self.dataset {
            DataSet::Filtered(data) => {
                // ZSet filtered data is stored as [member1, score1, member2, score2, ...]
                let data_index = index * 2;
                if data_index + 1 < data.len() {
                    Some(vec![data[data_index].clone(), data[data_index + 1].clone()])
                } else {
                    None
                }
            }
            _ => self.dataset.get_row(index),
        }
    }

    /// Override data_len to handle zset-specific filtered data format
    fn data_len(&self) -> usize {
        match &self.dataset {
            DataSet::Filtered(data) => data.len() / 2, // member-score pairs
            _ => self.dataset.len(),
        }
    }

    fn insert(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        // Expect data in pairs: [member1, score1, member2, score2, ...]
        let items: Vec<(f64, String)> = data
            .chunks(2)
            .filter_map(|chunk| {
                if chunk.len() == 2 {
                    if let Ok(score) = chunk[1].parse::<f64>() {
                        Some((score, chunk[0].clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        for (score, member) in &items {
            let _: () = redis::cmd("ZADD")
                .arg(key_prefix)
                .arg(*score)
                .arg(member)
                .query(conn)?;
        }
        Ok(())
    }

    fn delete(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        for member in data {
            let _: i32 = redis::cmd("ZREM").arg(key_prefix).arg(member).query(conn)?;
        }
        Ok(())
    }

    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        matches!(
            operator,
            ComparisonOperator::Equal | ComparisonOperator::In | ComparisonOperator::Like
        )
    }

    fn set_filtered_data(&mut self, data: Vec<String>) {
        self.dataset = DataSet::Filtered(data);
    }
}
