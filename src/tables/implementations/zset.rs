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
        limit_offset: &LimitOffsetInfo,
    ) -> Result<LoadDataResult, redis::RedisError> {
        let mut scan_builder = RedisScanBuilder::new_zset_scan(key_prefix);

        // Add pattern from the first LIKE condition and limit constraints
        if let Some(_pattern_condition) = scan_conditions.pattern_conditions.first() {
            if let Some(matcher) = &scan_conditions.pattern_matcher {
                scan_builder = scan_builder.with_pattern(matcher.get_pattern());
            }
        }

        // Add limit information to the scan builder
        scan_builder = scan_builder.with_limit(limit_offset.clone());

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

        // Apply LIMIT/OFFSET to filtered results
        if limit_offset.has_constraints() {
            // For zset data, we need to apply pagination at the member-score pair level
            let pairs: Vec<String> = filtered_data
                .chunks(2)
                .map(|chunk| format!("{}\t{}", chunk[0], chunk[1]))
                .collect();

            let paginated_pairs = limit_offset.apply_to_vec(pairs);

            filtered_data = paginated_pairs
                .into_iter()
                .flat_map(|pair| {
                    let parts: Vec<&str> = pair.split('\t').collect();
                    if parts.len() == 2 {
                        vec![parts[0].to_string(), parts[1].to_string()]
                    } else {
                        vec![]
                    }
                })
                .collect();
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
        limit_offset: &LimitOffsetInfo,
    ) -> Result<LoadDataResult, redis::RedisError> {
        if let Some(conditions) = conditions {
            let scan_conditions = extract_scan_conditions(conditions);

            // Check for SCAN-optimizable conditions first
            if scan_conditions.has_optimizable_conditions() {
                return self.load_with_scan_optimization(
                    conn,
                    key_prefix,
                    &scan_conditions,
                    limit_offset,
                );
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

        // ZSets support efficient range queries with LIMIT/OFFSET using ZRANGE
        let (start, end) = if limit_offset.has_constraints() {
            let offset = limit_offset.offset.unwrap_or(0) as isize;
            let limit = limit_offset.limit.unwrap_or(usize::MAX);
            let end_idx = if limit == usize::MAX {
                -1isize // Redis ZRANGE end=-1 means to the end of sorted set
            } else {
                offset + (limit as isize) - 1
            };
            (offset, end_idx)
        } else {
            (0, -1) // Get all elements
        };

        let result: Vec<(String, f64)> = redis::cmd("ZRANGE")
            .arg(key_prefix)
            .arg(start)
            .arg(end)
            .arg("WITHSCORES")
            .query(conn)?;

        if limit_offset.has_constraints() {
            // Convert to filtered format for efficient access
            let flat_data: Vec<String> = result
                .into_iter()
                .flat_map(|(member, score)| vec![member, score.to_string()])
                .collect();
            self.dataset = DataSet::Filtered(flat_data);
        } else {
            self.dataset = DataSet::Complete(DataContainer::ZSet(result));
        }
        Ok(LoadDataResult::FullyLoaded)
    }

    fn get_dataset(&self) -> &DataSet {
        &self.dataset
    }

    fn get_dataset_mut(&mut self) -> &mut DataSet {
        &mut self.dataset
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
}
