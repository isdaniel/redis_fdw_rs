use std::borrow::Cow;

use crate::{
    query::{
        limit::LimitOffsetInfo,
        pushdown_types::{ComparisonOperator, PushableCondition},
        scan_ops::{extract_scan_conditions, PatternMatcher, RedisScanBuilder, ScanConditions},
    },
    tables::{
        interface::RedisTableOperations,
        types::{DataContainer, DataSet, LoadDataResult, RowVec},
    },
};
use smallvec::smallvec;

/// Safe default limit for Redis LIMIT argument (works on both 32-bit and 64-bit).
/// On 64-bit this equals i64::MAX; on 32-bit it equals u32::MAX (usize::MAX).
const REDIS_LIMIT_MAX: usize = if (usize::MAX as u128) < (i64::MAX as u128) {
    usize::MAX
} else {
    i64::MAX as usize
};

/// Parse a ZRANGEBYSCORE bound string into f64 for comparison.
fn parse_bound(s: &str, default: f64) -> f64 {
    let trimmed = s.strip_prefix('(').unwrap_or(s);
    match trimmed {
        "-inf" => f64::NEG_INFINITY,
        "+inf" => f64::INFINITY,
        v => v.parse().unwrap_or(default),
    }
}

/// Redis Sorted Set table type
#[derive(Debug, Clone, Default)]
pub struct RedisZSetTable {
    pub dataset: DataSet,
    pub pushdown_column_index: usize,
    pub score_column_index: usize,
}

impl RedisZSetTable {
    pub fn new() -> Self {
        Self {
            dataset: DataSet::Empty,
            pushdown_column_index: 0,
            score_column_index: 1,
        }
    }

    fn load_with_score_range(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        score_conditions: &[&PushableCondition],
        limit_offset: &LimitOffsetInfo,
    ) -> Result<LoadDataResult, redis::RedisError> {
        let mut min_score = "-inf".to_string();
        let mut max_score = "+inf".to_string();
        let mut min_exclusive = false;
        let mut max_exclusive = false;

        for cond in score_conditions {
            match cond.operator {
                ComparisonOperator::GreaterThan => {
                    let new_val: f64 = cond.value.parse().unwrap_or(f64::NEG_INFINITY);
                    let cur_val = parse_bound(&min_score, f64::NEG_INFINITY);
                    if new_val > cur_val || (new_val == cur_val && !min_exclusive) {
                        min_score = cond.value.clone();
                        min_exclusive = true;
                    }
                }
                ComparisonOperator::GreaterThanOrEqual => {
                    let new_val: f64 = cond.value.parse().unwrap_or(f64::NEG_INFINITY);
                    let cur_val = parse_bound(&min_score, f64::NEG_INFINITY);
                    if new_val > cur_val {
                        min_score = cond.value.clone();
                        min_exclusive = false;
                    }
                }
                ComparisonOperator::LessThan => {
                    let new_val: f64 = cond.value.parse().unwrap_or(f64::INFINITY);
                    let cur_val = parse_bound(&max_score, f64::INFINITY);
                    if new_val < cur_val || (new_val == cur_val && !max_exclusive) {
                        max_score = cond.value.clone();
                        max_exclusive = true;
                    }
                }
                ComparisonOperator::LessThanOrEqual => {
                    let new_val: f64 = cond.value.parse().unwrap_or(f64::INFINITY);
                    let cur_val = parse_bound(&max_score, f64::INFINITY);
                    if new_val < cur_val {
                        max_score = cond.value.clone();
                        max_exclusive = false;
                    }
                }
                ComparisonOperator::Equal => {
                    let eq_val: f64 = cond.value.parse().unwrap_or(0.0);
                    let cur_min = parse_bound(&min_score, f64::NEG_INFINITY);
                    let cur_max = parse_bound(&max_score, f64::INFINITY);

                    // Only apply if equality value is within current bounds
                    if eq_val > cur_min || (eq_val == cur_min && !min_exclusive) {
                        min_score = cond.value.clone();
                        min_exclusive = false;
                    }
                    if eq_val < cur_max || (eq_val == cur_max && !max_exclusive) {
                        max_score = cond.value.clone();
                        max_exclusive = false;
                    }
                }
                _ => {}
            }
        }

        let final_min = if min_exclusive {
            format!("({}", min_score)
        } else {
            min_score
        };
        let final_max = if max_exclusive {
            format!("({}", max_score)
        } else {
            max_score
        };

        let mut cmd = redis::cmd("ZRANGEBYSCORE");
        cmd.arg(key_prefix)
            .arg(&final_min)
            .arg(&final_max)
            .arg("WITHSCORES");

        if limit_offset.has_constraints() {
            let offset = limit_offset.offset.unwrap_or(0);
            let limit = limit_offset.limit.unwrap_or(REDIS_LIMIT_MAX);
            cmd.arg("LIMIT").arg(offset).arg(limit);
        }

        let result: Vec<String> = cmd.query(conn)?;

        if result.is_empty() {
            self.dataset = DataSet::Empty;
            Ok(LoadDataResult::Empty)
        } else {
            self.dataset = DataSet::Filtered(result);
            Ok(LoadDataResult::FullyLoaded)
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
                    if condition.operator == ComparisonOperator::Like {
                        if let Some(matcher) = &scan_conditions.pattern_matcher {
                            if !matcher.matches(member) {
                                matches = false;
                                break;
                            }
                        }
                    }
                }

                // Check exact conditions
                for condition in &scan_conditions.exact_conditions {
                    if condition.operator == ComparisonOperator::Equal && member != &condition.value
                    {
                        matches = false;
                        break;
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
            let limit = limit_offset.limit.unwrap_or(REDIS_LIMIT_MAX);

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
            // Prioritize member equality lookups (ZSCORE is O(1)) over score-range (ZRANGEBYSCORE is O(log N + M))
            let target_idx = self.pushdown_column_index;
            let member_conditions: Vec<PushableCondition> = conditions
                .iter()
                .filter(|c| c.column_index == target_idx)
                .cloned()
                .collect();

            if !member_conditions.is_empty() {
                let scan_conditions = extract_scan_conditions(&member_conditions);

                if !scan_conditions.pattern_conditions.is_empty() {
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
                            let zmscore_result: Result<Vec<Option<f64>>, _> = redis::cmd("ZMSCORE")
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
                                            pipe.cmd("ZSCORE").arg(key_prefix).arg(*member);
                                        }
                                        pipe.query(conn)
                                    };

                                    match pipe_result {
                                        Ok(scores) => {
                                            for (member, score) in members.iter().zip(scores.iter())
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
                        _ => {} // Fall through to score-range check
                    }
                }
            }

            // Fallback: score-range conditions (ZRANGEBYSCORE is O(log N + M))
            let score_idx = self.score_column_index;
            let score_conditions: Vec<&PushableCondition> = conditions
                .iter()
                .filter(|c| c.column_index == score_idx)
                .collect();

            if !score_conditions.is_empty() {
                return self.load_with_score_range(
                    conn,
                    key_prefix,
                    &score_conditions,
                    limit_offset,
                );
            }
        }

        // ZSets support efficient range queries with LIMIT/OFFSET using ZRANGE
        let (start, end) = if limit_offset.has_constraints() {
            let offset = limit_offset.offset.unwrap_or(0) as isize;
            let limit = limit_offset.limit.unwrap_or(REDIS_LIMIT_MAX);
            let end_idx = if limit == REDIS_LIMIT_MAX {
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

    /// Override the default get_row implementation to handle zset-specific filtered data format
    #[inline]
    fn get_row(&self, index: usize) -> Option<RowVec<'_>> {
        match &self.dataset {
            DataSet::Filtered(data) => {
                // ZSet filtered data is stored as [member1, score1, member2, score2, ...]
                let data_index = index * 2;
                if data_index + 1 < data.len() {
                    Some(smallvec![
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
        // old_data: [member], new_data: [member, score]
        if new_data.len() >= 2 {
            let new_member = &new_data[0];
            let new_score: f64 = new_data[1]
                .parse()
                .map_err(|e: std::num::ParseFloatError| {
                    redis::RedisError::from((
                        redis::ErrorKind::InvalidClientConfig,
                        "Invalid score format",
                        e.to_string(),
                    ))
                })?;

            // If member name changed, use atomic pipeline (ZREM old + ZADD new)
            if let Some(old_member) = old_data.first() {
                if old_member != new_member {
                    redis::pipe()
                        .atomic()
                        .cmd("ZREM")
                        .arg(key_prefix)
                        .arg(old_member)
                        .cmd("ZADD")
                        .arg(key_prefix)
                        .arg(new_score)
                        .arg(new_member)
                        .query::<()>(conn)?;
                    return Ok(());
                }
            }

            // Score-only update (no rename needed)
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
            ComparisonOperator::Equal
                | ComparisonOperator::In
                | ComparisonOperator::Like
                | ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual
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
        // Filter out score conditions — Redis ZSCAN can only match on member names
        let target_idx = self.pushdown_column_index;
        let member_conditions: Option<Vec<&PushableCondition>> = conditions.map(|conds| {
            conds
                .iter()
                .filter(|c| c.column_index == target_idx)
                .collect()
        });
        let member_conds: Option<&[&PushableCondition]> = member_conditions.as_deref();

        let mut cmd = redis::cmd("ZSCAN");
        cmd.arg(key_prefix).arg(cursor);

        // Apply LIKE condition as MATCH pattern for server-side filtering
        let like_pattern = member_conds.and_then(|conds| {
            conds.iter().find_map(|c| {
                if c.operator == ComparisonOperator::Like {
                    Some(PatternMatcher::from_like_pattern(&c.value))
                } else {
                    None
                }
            })
        });
        if let Some(ref matcher) = like_pattern {
            cmd.arg("MATCH").arg(matcher.get_pattern());
        }
        cmd.arg("COUNT").arg(batch_size);

        let (new_cursor, flat_data): (u64, Vec<String>) = cmd.query(conn)?;

        // Apply member conditions as client-side post-filter
        // Note: Only the first LIKE condition was used for server-side MATCH;
        // all other conditions (including additional LIKEs) must be verified here
        let filtered: Vec<String> = if let Some(conds) = member_conds {
            if conds.is_empty() {
                flat_data
            } else {
                let first_like_value = conds.iter().find_map(|c| {
                    if c.operator == ComparisonOperator::Like {
                        Some(&c.value)
                    } else {
                        None
                    }
                });
                let extra_like_matchers: Vec<(&str, PatternMatcher)> = conds
                    .iter()
                    .filter(|c| {
                        c.operator == ComparisonOperator::Like && Some(&c.value) != first_like_value
                    })
                    .map(|c| {
                        (
                            c.value.as_str(),
                            PatternMatcher::from_like_pattern(&c.value),
                        )
                    })
                    .collect();
                let in_value_sets: Vec<Vec<&str>> = conds
                    .iter()
                    .map(|c| {
                        if c.operator == ComparisonOperator::In {
                            c.value.split(',').collect()
                        } else {
                            Vec::new()
                        }
                    })
                    .collect();
                flat_data
                    .chunks(2)
                    .filter(|chunk| {
                        if chunk.len() == 2 {
                            let member = &chunk[0];
                            conds.iter().enumerate().all(|(i, c)| match c.operator {
                                ComparisonOperator::Equal => member == &c.value,
                                ComparisonOperator::NotEqual => member != &c.value,
                                ComparisonOperator::In => {
                                    in_value_sets[i].contains(&member.as_str())
                                }
                                ComparisonOperator::Like => {
                                    if Some(&c.value) == first_like_value {
                                        true // handled by MATCH
                                    } else {
                                        extra_like_matchers
                                            .iter()
                                            .find(|(v, _)| *v == c.value.as_str())
                                            .is_some_and(|(_, m)| m.matches(member))
                                    }
                                }
                                _ => true,
                            })
                        } else {
                            false
                        }
                    })
                    .flat_map(|chunk| chunk.iter().cloned())
                    .collect()
            }
        } else {
            flat_data
        };

        let row_count = filtered.len() / 2;
        self.dataset = if filtered.is_empty() {
            DataSet::Empty
        } else {
            DataSet::Filtered(filtered)
        };
        Ok((new_cursor, row_count))
    }

    fn configure(
        &mut self,
        _column_names: &[String],
        pushdown_column_index: usize,
        score_column_index: Option<usize>,
    ) {
        self.pushdown_column_index = pushdown_column_index;
        self.score_column_index = score_column_index.unwrap_or(pushdown_column_index + 1);
    }

    fn load_multi_key_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        keys: &[String],
    ) -> Result<Vec<String>, redis::RedisError> {
        const PER_KEY_WARN_THRESHOLD: usize = 200_000;

        let pipe_result: Result<Vec<Vec<(String, f64)>>, _> = {
            let mut pipe = redis::pipe();
            for key in keys {
                pipe.cmd("ZRANGE")
                    .arg(key)
                    .arg(0i64)
                    .arg(-1i64)
                    .arg("WITHSCORES");
            }
            pipe.query(conn)
        };

        let results = match pipe_result {
            Ok(r) => r,
            Err(_) => {
                let mut results = Vec::with_capacity(keys.len());
                for key in keys {
                    let r: Vec<(String, f64)> = redis::cmd("ZRANGE")
                        .arg(key)
                        .arg(0i64)
                        .arg(-1i64)
                        .arg("WITHSCORES")
                        .query(conn)?;
                    results.push(r);
                }
                results
            }
        };

        let mut all_rows = Vec::with_capacity(keys.len() * self.multi_key_columns_per_row());
        for (key, members) in keys.iter().zip(results) {
            pgrx::check_for_interrupts!();
            if members.len() > PER_KEY_WARN_THRESHOLD {
                pgrx::warning!(
                    "Redis FDW: key '{}' contains {} elements, consider using LIMIT",
                    key,
                    members.len()
                );
            }
            for (member, score) in members {
                all_rows.push(key.clone());
                all_rows.push(member);
                all_rows.push(score.to_string());
            }
        }
        Ok(all_rows)
    }

    fn clear(&mut self) {
        self.dataset = DataSet::default();
    }

    fn redis_type_name(&self) -> &'static str {
        "zset"
    }

    fn set_filtered_data(&mut self, data: Vec<String>) {
        self.dataset = DataSet::Filtered(data);
    }

    fn multi_key_columns_per_row(&self) -> usize {
        3
    }
}
