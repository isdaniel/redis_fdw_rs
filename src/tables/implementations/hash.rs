use std::collections::HashMap;

use pgrx::info;

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

    /// Load data with HSCAN optimization for pattern matching
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
                // Use HSCAN with MATCH to find matching fields
                let matching_fields: Vec<String> = RedisScanBuilder::new_hash_scan(key_prefix)
                    .with_pattern(pattern_matcher.get_pattern())
                    .with_limit(limit_offset.clone())
                    .execute_all(conn)?;
                
                if matching_fields.is_empty() {
                    self.dataset = DataSet::Empty;
                    return Ok(LoadDataResult::Empty);
                }

                self.dataset = DataSet::Filtered(matching_fields.clone());
                Ok(LoadDataResult::PushdownApplied(matching_fields))
            } else {
                // Exact field match
                let value: Option<String> = redis::cmd("HGET")
                    .arg(key_prefix)
                    .arg(&pattern)
                    .query(conn)?;

                if let Some(v) = value {
                    let result = vec![pattern.clone(), v];
                    self.dataset = DataSet::Filtered(result.clone());
                    Ok(LoadDataResult::PushdownApplied(result))
                } else {
                    self.dataset = DataSet::Empty;
                    Ok(LoadDataResult::Empty)
                }
            }
        } else {
            // No pattern available, fallback to regular load
            let hash_data: HashMap<String, String> =
                redis::cmd("HGETALL").arg(key_prefix).query(conn)?;
            let data_vec: Vec<(String, String)> = hash_data.into_iter().collect();
            self.dataset = DataSet::Complete(DataContainer::Hash(data_vec));
            Ok(LoadDataResult::FullyLoaded)
        }
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

            // Check for SCAN-optimizable conditions first
            if scan_conditions.has_optimizable_conditions() {
                let res = self.load_with_scan_optimization(
                    conn,
                    key_prefix,
                    &scan_conditions,
                    limit_offset,
                );
                info!("Load with scan optimization result: {:?}", res);
                return res;
            }

            // Legacy optimization for non-pattern conditions
            // todo fix bug
            if !conditions.is_empty() {
                for condition in conditions {
                    match condition.operator {
                        ComparisonOperator::Equal => {
                            pgrx::log!("Applying pushdown for condition: {:?}", condition);
                            let value: Option<String> = redis::cmd("HGET")
                                .arg(key_prefix)
                                .arg(&condition.value)
                                .query(conn)?;

                            return if let Some(v) = value {
                                let filtered_data = vec![condition.value.clone(), v];
                                self.dataset = DataSet::Filtered(filtered_data.clone());
                                Ok(LoadDataResult::PushdownApplied(filtered_data))
                            } else {
                                self.dataset = DataSet::Empty;
                                Ok(LoadDataResult::Empty)
                            };
                        }
                        ComparisonOperator::In => {
                            // HMGET for multiple fields
                            let fields: Vec<&str> = condition.value.split(',').collect();
                            let values: Vec<Option<String>> = redis::cmd("HMGET")
                                .arg(key_prefix)
                                .arg(&fields)
                                .query(conn)?;

                            let mut result = Vec::new();
                            for (i, value) in values.iter().enumerate() {
                                if let Some(v) = value {
                                    result.push(fields[i].to_string());
                                    result.push(v.clone());
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

        // Load all data (either no conditions or pushdown not applicable)
        let hash_data: HashMap<String, String> =
            redis::cmd("HGETALL").arg(key_prefix).query(conn)?;
        let data_vec: Vec<(String, String)> = hash_data.into_iter().collect();
        self.dataset = DataSet::Complete(DataContainer::Hash(data_vec));
        Ok(LoadDataResult::FullyLoaded)
    }

    fn get_dataset(&self) -> &DataSet {
        &self.dataset
    }

    fn get_dataset_mut(&mut self) -> &mut DataSet {
        &mut self.dataset
    }

    /// Override the default get_row implementation to handle hash-specific filtered data format
    fn get_row(&self, index: usize) -> Option<Vec<String>> {
        info!("Getting row for hash table self: {:?}", self);
        match &self.dataset {
            DataSet::Filtered(data) => {
                // Hash filtered data is stored as [key1, value1, key2, value2, ...]
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

    /// Override data_len to handle hash-specific filtered data format
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

    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        matches!(
            operator,
            ComparisonOperator::Equal | ComparisonOperator::In | ComparisonOperator::Like
        )
    }
}
