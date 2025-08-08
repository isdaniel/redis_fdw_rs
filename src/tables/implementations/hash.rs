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
                let matching_fields: Vec<String> = RedisScanBuilder::new_hash_scan(key_prefix)
                    .with_pattern(pattern_matcher.get_pattern())
                    .with_limit(limit_offset.clone())
                    .execute_all(conn)?;

                if matching_fields.is_empty() {
                    self.dataset = DataSet::Empty;
                    return Ok(LoadDataResult::Empty);
                }

                self.dataset = DataSet::Filtered(matching_fields.clone());
                return Ok(LoadDataResult::PushdownApplied(matching_fields));
            }
            info!("scan_conditions:{:?}",scan_conditions);
            // exact match case
            return self.hget_exact(conn, key_prefix, &pattern);
        }

        // no pattern — fallback
        self.hgetall_all(conn, key_prefix)
    }

     fn hget_exact(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        field: &str,
    ) -> Result<LoadDataResult, redis::RedisError> {
        let value: Option<String> = redis::cmd("HGET")
            .arg(key_prefix)
            .arg(field)
            .query(conn)?;

        if let Some(v) = value {
            let result = vec![field.to_string(), v];
            self.dataset = DataSet::Filtered(result.clone());
            Ok(LoadDataResult::PushdownApplied(result))
        } else {
            self.dataset = DataSet::Empty;
            Ok(LoadDataResult::Empty)
        }
    }

    /// Helper: Fetch multiple fields
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
        self.dataset = DataSet::Filtered(result.clone());
        Ok(LoadDataResult::PushdownApplied(result))
    }

    /// Helper: Load full hash
    fn hgetall_all(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
    ) -> Result<LoadDataResult, redis::RedisError> {
        let hash_data: HashMap<String, String> =
            redis::cmd("HGETALL").arg(key_prefix).query(conn)?;
        let data_vec: Vec<(String, String)> = hash_data.into_iter().collect();
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
                return self.load_with_scan_optimization(conn, key_prefix, &scan_conditions, limit_offset);
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

    fn get_dataset_mut(&mut self) -> &mut DataSet {
        &mut self.dataset
    }

    /// Override the default get_row implementation to handle hash-specific filtered data format
    fn get_row(&self, index: usize) -> Option<Vec<String>> {
        //info!("Getting row for hash table self: {:?}", self);
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
