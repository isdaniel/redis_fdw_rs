use std::borrow::Cow;

use crate::{
    query::{
        limit::LimitOffsetInfo,
        pushdown_types::{ComparisonOperator, PushableCondition},
        scan_ops::{extract_scan_conditions, PatternMatcher, RedisScanBuilder},
    },
    tables::{
        interface::RedisTableOperations,
        types::{DataContainer, DataSet, LoadDataResult},
    },
};

/// Redis Hash table type
#[derive(Debug, Clone, Default)]
pub struct RedisHashTable {
    pub dataset: DataSet,
}

impl RedisHashTable {
    pub fn new() -> Self {
        Self {
            dataset: DataSet::Empty,
        }
    }

    #[allow(dead_code)]
    fn load_with_scan_optimization(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        scan_conditions: &crate::query::scan_ops::ScanConditions,
        limit_offset: &LimitOffsetInfo,
    ) -> Result<LoadDataResult, redis::RedisError> {
        if let Some(pattern) = scan_conditions.get_primary_pattern() {
            let pattern_matcher = PatternMatcher::from_like_pattern(&pattern);

            if pattern_matcher.requires_scan() {
                let matching_fields: Vec<String> = RedisScanBuilder::new_hash_scan(key_prefix)
                    .with_pattern(pattern_matcher.get_pattern())
                    .with_limit(limit_offset.clone())
                    .execute_all(conn)?;

                if matching_fields.is_empty() {
                    self.dataset = DataSet::Empty;
                    return Ok(LoadDataResult::Empty);
                }

                self.dataset = DataSet::Filtered(matching_fields);
                return Ok(LoadDataResult::FullyLoaded);
            }
            return self.hget_exact(conn, key_prefix, &pattern);
        }

        self.hgetall_all(conn, key_prefix)
    }

    #[allow(dead_code)]
    fn hget_exact(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        field: &str,
    ) -> Result<LoadDataResult, redis::RedisError> {
        let value: Option<String> = redis::cmd("HGET").arg(key_prefix).arg(field).query(conn)?;

        if let Some(v) = value {
            let result = vec![field.to_string(), v];
            self.dataset = DataSet::Filtered(result);
            Ok(LoadDataResult::FullyLoaded)
        } else {
            self.dataset = DataSet::Empty;
            Ok(LoadDataResult::Empty)
        }
    }

    #[allow(dead_code)]
    fn hmget_fields(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        fields: &[&str],
    ) -> Result<LoadDataResult, redis::RedisError> {
        let values: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(key_prefix)
            .arg(fields)
            .query(conn)?;

        let mut result = Vec::new();
        for (i, value) in values.iter().enumerate() {
            if let Some(v) = value {
                result.push(fields[i].to_string());
                result.push(v.clone());
            }
        }
        self.dataset = DataSet::Filtered(result);
        Ok(LoadDataResult::FullyLoaded)
    }

    #[allow(dead_code)]
    fn hgetall_all(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
    ) -> Result<LoadDataResult, redis::RedisError> {
        let data_vec: Vec<(String, String)> = redis::cmd("HGETALL").arg(key_prefix).query(conn)?;
        self.dataset = DataSet::Complete(DataContainer::Hash(data_vec));
        Ok(LoadDataResult::FullyLoaded)
    }
}

impl RedisTableOperations for RedisHashTable {
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
        limit_offset: &LimitOffsetInfo,
    ) -> Result<LoadDataResult, redis::RedisError> {
        if let Some(conditions) = conditions {
            let scan_conditions = extract_scan_conditions(conditions);

            if scan_conditions.has_optimizable_conditions() {
                return self.load_with_scan_optimization(
                    conn,
                    key_prefix,
                    &scan_conditions,
                    limit_offset,
                );
            }

            // legacy non-pattern pushdowns
            if let Some(first) = conditions.first() {
                match first.operator {
                    ComparisonOperator::Equal => {
                        return self.hget_exact(conn, key_prefix, &first.value);
                    }
                    ComparisonOperator::In => {
                        let fields: Vec<&str> = first.value.split(',').collect();
                        return self.hmget_fields(conn, key_prefix, &fields);
                    }
                    _ => {} // fallback
                }
            }
        }

        // no conditions or pushdown not possible
        self.hgetall_all(conn, key_prefix)
    }

    fn get_dataset(&self) -> &DataSet {
        &self.dataset
    }

    /// Override the default get_row implementation to handle hash-specific filtered data format
    #[inline]
    fn get_row(&self, index: usize) -> Option<Vec<Cow<'_, str>>> {
        match &self.dataset {
            DataSet::Filtered(data) => {
                // Hash filtered data is stored as [key1, value1, key2, value2, ...]
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

    /// Override data_len to handle hash-specific filtered data format
    #[inline]
    fn data_len(&self) -> usize {
        match &self.dataset {
            DataSet::Filtered(data) => data.len() / 2, // key-value pairs
            _ => self.dataset.len(),
        }
    }

    fn insert(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        let fields: Vec<(String, String)> = data
            .chunks(2)
            .filter_map(|chunk| {
                if chunk.len() == 2 {
                    Some((chunk[0].clone(), chunk[1].clone()))
                } else {
                    None
                }
            })
            .collect();

        if !fields.is_empty() {
            let _: () = redis::cmd("HSET")
                .arg(key_prefix)
                .arg(&fields)
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
        if !data.is_empty() {
            let _: () = redis::cmd("HDEL").arg(key_prefix).arg(data).query(conn)?;
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
        // new_data format: [field, new_value]
        if new_data.len() >= 2 {
            let new_field = &new_data[0];

            // If field name changed, use pipeline for atomicity (HDEL + HSET)
            if let Some(old_field) = old_data.first() {
                if old_field != new_field {
                    redis::pipe()
                        .atomic()
                        .cmd("HDEL")
                        .arg(key_prefix)
                        .arg(old_field)
                        .cmd("HSET")
                        .arg(key_prefix)
                        .arg(new_field)
                        .arg(&new_data[1])
                        .query::<()>(conn)?;
                    return Ok(());
                }
            }

            let _: () = redis::cmd("HSET")
                .arg(key_prefix)
                .arg(new_field)
                .arg(&new_data[1])
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

    fn load_batch(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        cursor: u64,
        batch_size: usize,
        conditions: Option<&[PushableCondition]>,
    ) -> Result<(u64, usize), redis::RedisError> {
        // Only apply conditions on the field/key column (first column) to Redis
        // Value column conditions must be handled by PG post-filter
        let field_conditions: Option<Vec<&PushableCondition>> =
            conditions.map(|conds| conds.iter().filter(|c| c.column_name != "value").collect());
        let field_conds: Option<&[&PushableCondition]> = field_conditions.as_deref();

        let mut cmd = redis::cmd("HSCAN");
        cmd.arg(key_prefix).arg(cursor);

        // Apply LIKE condition as MATCH pattern for server-side filtering
        let like_pattern = field_conds.and_then(|conds| {
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

        let (new_cursor, pairs): (u64, Vec<(String, String)>) = cmd.query(conn)?;

        // Apply field conditions as client-side post-filter
        // Note: Only the first LIKE condition was used for server-side MATCH;
        // all other conditions (including additional LIKEs) must be verified here
        let filtered: Vec<(String, String)> = if let Some(conds) = field_conds {
            if conds.is_empty() {
                pairs
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
                pairs
                    .into_iter()
                    .filter(|(field, _)| {
                        conds.iter().all(|c| match c.operator {
                            ComparisonOperator::Equal => field == &c.value,
                            ComparisonOperator::NotEqual => field != &c.value,
                            ComparisonOperator::Like => {
                                if Some(&c.value) == first_like_value {
                                    true // handled by MATCH
                                } else {
                                    extra_like_matchers
                                        .iter()
                                        .find(|(v, _)| *v == c.value.as_str())
                                        .is_some_and(|(_, m)| m.matches(field))
                                }
                            }
                            _ => true,
                        })
                    })
                    .collect()
            }
        } else {
            pairs
        };

        let row_count = filtered.len();
        self.dataset = if filtered.is_empty() {
            DataSet::Empty
        } else {
            DataSet::Complete(DataContainer::Hash(filtered))
        };
        Ok((new_cursor, row_count))
    }
}
