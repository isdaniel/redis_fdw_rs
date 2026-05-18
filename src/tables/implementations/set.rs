use crate::{
    query::{
        limit::LimitOffsetInfo,
        pushdown_types::{ComparisonOperator, PushableCondition},
        scan_ops::{extract_scan_conditions, PatternMatcher, RedisScanBuilder},
    },
    tables::{
        interface::RedisTableOperations,
        types::{DataSet, LoadDataResult},
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

    fn match_equal(
        &self,
        conn: &mut dyn redis::ConnectionLike,
        key: &str,
        member: &str,
    ) -> redis::RedisResult<Vec<String>> {
        let exists: bool = redis::cmd("SISMEMBER").arg(key).arg(member).query(conn)?;
        Ok(if exists {
            vec![member.to_string()]
        } else {
            vec![]
        })
    }

    fn match_in(
        &self,
        conn: &mut dyn redis::ConnectionLike,
        key: &str,
        members: &[&str],
    ) -> redis::RedisResult<Vec<String>> {
        if members.is_empty() {
            return Ok(vec![]);
        }

        // Try SMISMEMBER (Redis 6.2+) — single command, single round-trip
        let smismember_result: Result<Vec<bool>, _> =
            redis::cmd("SMISMEMBER").arg(key).arg(members).query(conn);

        match smismember_result {
            Ok(results) => {
                let matched: Vec<String> = members
                    .iter()
                    .zip(results.iter())
                    .filter(|(_, &exists)| exists)
                    .map(|(&m, _)| m.to_string())
                    .collect();
                Ok(matched)
            }
            Err(_) => {
                // Fallback: try pipeline first, then individual commands for cluster
                let pipe_result: Result<Vec<bool>, _> = {
                    let mut pipe = redis::pipe();
                    for m in members {
                        pipe.cmd("SISMEMBER").arg(key).arg(*m);
                    }
                    pipe.query(conn)
                };

                match pipe_result {
                    Ok(results) => {
                        let matched: Vec<String> = members
                            .iter()
                            .zip(results.iter())
                            .filter(|(_, &exists)| exists)
                            .map(|(&m, _)| m.to_string())
                            .collect();
                        Ok(matched)
                    }
                    Err(_) => {
                        // Final fallback: individual SISMEMBER commands (cluster mode)
                        let mut matched = Vec::new();
                        for m in members {
                            let exists: bool =
                                redis::cmd("SISMEMBER").arg(key).arg(*m).query(conn)?;
                            if exists {
                                matched.push(m.to_string());
                            }
                        }
                        Ok(matched)
                    }
                }
            }
        }
    }

    fn match_like(
        &self,
        conn: &mut dyn redis::ConnectionLike,
        key: &str,
        pattern: &str,
        limit_offset: &LimitOffsetInfo,
    ) -> redis::RedisResult<Vec<String>> {
        let matcher = PatternMatcher::from_like_pattern(pattern);
        if matcher.requires_scan() {
            RedisScanBuilder::new_set_scan(key)
                .with_pattern(matcher.get_pattern())
                .with_limit(limit_offset.clone())
                .execute_all(conn)
        } else {
            self.match_equal(conn, key, pattern)
        }
    }

    fn all_members(
        &self,
        conn: &mut dyn redis::ConnectionLike,
        key: &str,
    ) -> redis::RedisResult<Vec<String>> {
        redis::cmd("SMEMBERS").arg(key).query(conn)
    }

    fn apply_conditions(
        &self,
        conn: &mut dyn redis::ConnectionLike,
        key: &str,
        conditions: &[PushableCondition],
        limit_offset: &LimitOffsetInfo,
    ) -> redis::RedisResult<Vec<String>> {
        let mut matched: Option<Vec<String>> = None;

        for cond in conditions {
            let matches = match cond.operator {
                ComparisonOperator::Equal => self.match_equal(conn, key, &cond.value)?,
                ComparisonOperator::In => {
                    let list: Vec<&str> = cond.value.split(',').collect();
                    self.match_in(conn, key, &list)?
                }
                ComparisonOperator::Like => {
                    self.match_like(conn, key, &cond.value, limit_offset)?
                }
                _ => self.all_members(conn, key)?,
            };

            matched = match matched {
                Some(prev) => Some(prev.into_iter().filter(|m| matches.contains(m)).collect()),
                None => Some(matches),
            };

            if matched.as_ref().is_some_and(|v| v.is_empty()) {
                break; // short-circuit if no match left
            }
        }

        Ok(matched.unwrap_or_default())
    }
}

impl RedisTableOperations for RedisSetTable {
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key: &str,
        conditions: Option<&[PushableCondition]>,
        limit_offset: &LimitOffsetInfo,
    ) -> redis::RedisResult<LoadDataResult> {
        let members = if let Some(conds) = conditions {
            let scan_conditions = extract_scan_conditions(conds);

            if scan_conditions.has_optimizable_conditions() {
                let pattern = scan_conditions.get_primary_pattern().unwrap();
                self.match_like(conn, key, &pattern, limit_offset)?
            } else {
                self.apply_conditions(conn, key, conds, limit_offset)?
            }
        } else {
            self.all_members(conn, key)?
        };

        self.dataset = if members.is_empty() {
            DataSet::Empty
        } else {
            DataSet::Filtered(members)
        };

        Ok(if self.dataset.len() == 0 {
            LoadDataResult::Empty
        } else {
            // Return a reference-like result — the data is now owned by self.dataset
            LoadDataResult::FullyLoaded
        })
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
        if !data.is_empty() {
            let _added: i32 = redis::cmd("SADD").arg(key_prefix).arg(data).query(conn)?;
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
            let _: i32 = redis::cmd("SREM").arg(key_prefix).arg(data).query(conn)?;
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
        if let (Some(old_member), Some(new_member)) = (old_data.first(), new_data.first()) {
            if old_member == new_member {
                return Ok(());
            }
            // Atomic swap via pipeline: SREM old + SADD new
            redis::pipe()
                .atomic()
                .cmd("SREM")
                .arg(key_prefix)
                .arg(old_member)
                .cmd("SADD")
                .arg(key_prefix)
                .arg(new_member)
                .query::<()>(conn)?;
        }
        Ok(())
    }

    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        matches!(
            operator,
            ComparisonOperator::Equal | ComparisonOperator::In | ComparisonOperator::Like
        )
    }

    fn load_batch(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key: &str,
        cursor: u64,
        batch_size: usize,
        conditions: Option<&[PushableCondition]>,
    ) -> Result<(u64, usize), redis::RedisError> {
        let mut cmd = redis::cmd("SSCAN");
        cmd.arg(key).arg(cursor);

        // Apply LIKE condition as MATCH pattern for server-side filtering
        let like_pattern = conditions.and_then(|conds| {
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

        let (new_cursor, members): (u64, Vec<String>) = cmd.query(conn)?;

        // Apply conditions as client-side post-filter
        // Note: Only the first LIKE condition was used for server-side MATCH;
        // all other conditions (including additional LIKEs) must be verified here
        let filtered: Vec<String> = if let Some(conds) = conditions {
            if conds.is_empty() {
                members
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
                members
                    .into_iter()
                    .filter(|member| {
                        conds.iter().enumerate().all(|(i, c)| match c.operator {
                            ComparisonOperator::Equal => member == &c.value,
                            ComparisonOperator::NotEqual => member != &c.value,
                            ComparisonOperator::In => in_value_sets[i].contains(&member.as_str()),
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
                    })
                    .collect()
            }
        } else {
            members
        };

        let row_count = filtered.len();
        self.dataset = if filtered.is_empty() {
            DataSet::Empty
        } else {
            DataSet::Filtered(filtered)
        };
        Ok((new_cursor, row_count))
    }
}
