use std::collections::HashMap;
use redis::Commands;

use crate::redis_fdw::{
    tables::interface::RedisTableOperations,
    pushdown::{PushableCondition, ComparisonOperator}
};

/// Redis Hash table type
#[derive(Debug, Clone, Default)]
pub struct RedisHashTable {
    pub data: Vec<(String, String)>,
}

impl RedisHashTable {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }
}

impl RedisTableOperations for RedisHashTable {
    fn load_data(&mut self, conn: &mut redis::Connection, key_prefix: &str, conditions: Option<&[PushableCondition]>) -> Result<Option<Vec<String>>, redis::RedisError> {
        if let Some(conditions) = conditions {
            if !conditions.is_empty() {
                // Apply hash-specific pushdown optimizations
                for condition in conditions {
                    match condition.operator {
                        ComparisonOperator::Equal => {
                            pgrx::info!("Applying pushdown for condition: {:?}", condition);
                            let value: Option<String> = redis::cmd("HGET")
                                .arg(key_prefix)
                                .arg(&condition.value)
                                .query(conn)?;
                            
                            return if let Some(v) = value {
                                Ok(Some(vec![condition.value.clone(), v]))
                            } else {
                                Ok(Some(vec![]))
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
                            return Ok(Some(result));
                        }
                        _ => {} // Fall back to full scan
                    }
                }
            }
        }

        // Load all data (either no conditions or pushdown not applicable)
        let hash_data: HashMap<String, String> = conn.hgetall(key_prefix)?;
        self.data = hash_data.into_iter().collect();
        Ok(None) // Return None to indicate data was loaded into internal storage
    }
    
    fn data_len(&self, filtered_data: Option<&[String]>) -> usize {
        if let Some(filtered_data) = filtered_data {
            filtered_data.len() / 2 // key-value pairs
        } else {
            self.data.len()
        }
    }
    
    fn get_row(&self, index: usize, filtered_data: Option<&[String]>) -> Option<Vec<String>> {
        if let Some(filtered_data) = filtered_data {
            // Hash data is stored as [key1, value1, key2, value2, ...]
            let data_index = index * 2;
            if data_index + 1 < filtered_data.len() {
                Some(vec![
                    filtered_data[data_index].clone(),
                    filtered_data[data_index + 1].clone(),
                ])
            } else {
                None
            }
        } else {
            self.data.get(index).map(|(k, v)| vec![k.clone(), v.clone()])
        }
    }
    
    fn insert(&mut self, conn: &mut redis::Connection, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
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
            let _: () = conn.hset_multiple(key_prefix, &fields)?;
            self.data.extend(fields);
        }
        Ok(())
    }
    
    fn delete(&mut self, conn: &mut redis::Connection, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        if !data.is_empty() {
            let _: () = redis::cmd("HDEL")
                .arg(key_prefix)
                .arg(data)
                .query(conn)?;
            
            // Remove from local data
            self.data.retain(|(k, _)| !data.contains(k));
        }
        Ok(())
    }
    
    fn update(&mut self, conn: &mut redis::Connection, key_prefix: &str, _old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        // For hash update, treat it as insert (HSET overwrites)
        self.insert(conn, key_prefix, new_data)
    }

    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        matches!(operator, ComparisonOperator::Equal | ComparisonOperator::In)
    }
}