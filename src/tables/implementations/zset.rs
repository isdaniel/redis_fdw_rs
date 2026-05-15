use std::borrow::Cow;

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
        let all_members: Vec<String> = scan_builder
            .with_limit(limit_offset.clone())
            .execute_all(conn)?;

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

        // Apply LIMIT/OFFSET to filtered results at the pair level
        if limit_offset.has_constraints() {
            let offset = limit_offset.offset.unwrap_or(0);
            let limit = limit_offset.limit.unwrap_or(usize::MAX);

            // filtered_data is [member1, score1, member2, score2, ...]
            // Apply offset/limit at pair granularity (2 elements per pair)
            let pair_start = offset * 2;
            let pair_count = limit * 2;

            if pair_start >= filtered_data.len() {
                filtered_data.clear();
            } else {
                let pair_end = (pair_start + pair_count).min(filtered_data.len());
                filtered_data = filtered_data[pair_start..pair_end].to_vec();
            }
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

impl RedisTableOperations for RedisZSetTable {
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
        limit_offset: &LimitOffsetInfo,
    ) -> Result<LoadDataResult, redis::RedisError> {
        if let Some(conditions) = conditions {
            // For ZSet, only pushdown conditions on the member column (first column)
            // Score conditions are left to PostgreSQL's post-filter
            let member_conditions: Vec<PushableCondition> = conditions
                .iter()
                .filter(|c| c.column_name != "score")
                .cloned()
                .collect();

            if !member_conditions.is_empty() {
                let scan_conditions = extract_scan_conditions(&member_conditions);

                // Check for SCAN-optimizable conditions first
                if scan_conditions.has_optimizable_conditions() {
                    return self.load_with_scan_optimization(
                        conn,
                        key_prefix,
                        &scan_conditions,
                        limit_offset,
                    );
                }

                // Direct member lookups
                for condition in &member_conditions {
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
                                self.dataset = DataSet::Filtered(filtered_data);
                                Ok(LoadDataResult::FullyLoaded)
                            } else {
                                self.dataset = DataSet::Empty;
                                Ok(LoadDataResult::Empty)
                            };
                        }
                        ComparisonOperator::In => {
                            // Check multiple members using ZMSCORE (Redis 6.2+) or pipeline
                            let members: Vec<&str> = condition.value.split(',').collect();
                            let mut result = Vec::new();

                            // Try ZMSCORE first (single command, single round-trip)
                            let zmscore_result: Result<Vec<Option<f64>>, _> =
                                redis::cmd("ZMSCORE")
                                    .arg(key_prefix)
                                    .arg(&members)
                                    .query(conn);

                            match zmscore_result {
                                Ok(scores) => {
                                    for (member, score) in members.iter().zip(scores.iter()) {
                                        if let Some(s) = score {
                                            result.push(member.to_string());
                                            result.push(s.to_string());
                                        }
                                    }
                                }
                                Err(_) => {
                                    // Try pipeline first, then individual commands for cluster
                                    let pipe_result: Result<Vec<Option<f64>>, _> = {
                                        let mut pipe = redis::pipe();
                                        for member in &members {
                                            pipe.cmd("ZSCORE")
                                                .arg(key_prefix)
                                                .arg(*member);
                                        }
                                        pipe.query(conn)
                                    };

                                    match pipe_result {
                                        Ok(scores) => {
                                            for (member, score) in
                                                members.iter().zip(scores.iter())
                                            {
                                                if let Some(s) = score {
                                                    result.push(member.to_string());
                                                    result.push(s.to_string());
                                                }
                                            }
                                        }
                                        Err(_) => {
                                            // Final fallback: individual ZSCORE (cluster mode)
                                            for member in &members {
                                                let score: Option<f64> = redis::cmd("ZSCORE")
                                                    .arg(key_prefix)
                                                    .arg(*member)
                                                    .query(conn)?;
                                                if let Some(s) = score {
                                                    result.push(member.to_string());
                                                    result.push(s.to_string());
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            return if result.is_empty() {
                                self.dataset = DataSet::Empty;
                                Ok(LoadDataResult::Empty)
                            } else {
                                self.dataset = DataSet::Filtered(result);
                                Ok(LoadDataResult::FullyLoaded)
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
    #[inline]
    fn get_row(&self, index: usize) -> Option<Vec<Cow<'_, str>>> {
        match &self.dataset {
            DataSet::Filtered(data) => {
                // ZSet filtered data is stored as [member1, score1, member2, score2, ...]
                let data_index = index * 2;
                if data_index + 1 < data.len() {
                    Some(vec![
                        Cow::Borrowed(data[data_index].as_str()),
                        Cow::Borrowed(data[data_index + 1].as_str()),
                    ])
                } else {
                    None
                }
            }
            _ => self.dataset.get_row(index),
        }
    }

    /// Override data_len to handle zset-specific filtered data format
    #[inline]
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
        let items: Vec<(f64, &str)> = data
            .chunks(2)
            .filter_map(|chunk| {
                if chunk.len() == 2 {
                    if let Ok(score) = chunk[1].parse::<f64>() {
                        Some((score, chunk[0].as_str()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        if !items.is_empty() {
            // Single ZADD with all score-member pairs
            let mut cmd = redis::cmd("ZADD");
            cmd.arg(key_prefix);
            for (score, member) in &items {
                cmd.arg(*score).arg(*member);
            }
            let _: () = cmd.query(conn)?;
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
            // Single ZREM with all members
            let _: i32 = redis::cmd("ZREM").arg(key_prefix).arg(data).query(conn)?;
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
        // old_data: [member, score], new_data: [member, score]
        if new_data.len() >= 2 {
            let new_member = &new_data[0];
            let new_score: f64 = new_data[1].parse().unwrap_or(0.0);

            // If member name changed, remove old member first
            if let Some(old_member) = old_data.first() {
                if old_member != new_member {
                    let _: i32 = redis::cmd("ZREM")
                        .arg(key_prefix)
                        .arg(old_member)
                        .query(conn)?;
                }
            }

            // ZADD with new score (works for both new member and score-only update)
            let _: () = redis::cmd("ZADD")
                .arg(key_prefix)
                .arg(new_score)
                .arg(new_member)
                .query(conn)?;
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
