use crate::redis_fdw::{
    tables::interface::RedisTableOperations,
    pushdown::{PushableCondition, ComparisonOperator}
};
use redis::Commands;

/// Redis List table type
#[derive(Debug, Clone, Default)]
pub struct RedisListTable {
    pub data: Vec<String>,
}

impl RedisListTable {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }
}

impl RedisTableOperations for RedisListTable {
    fn load_data(&mut self, conn: &mut redis::Connection, key_prefix: &str, _conditions: Option<&[PushableCondition]>) -> Result<Option<Vec<String>>, redis::RedisError> {
        // Lists don't have efficient filtering in Redis
        // Fall back to loading all data
        // Load all data into internal storage
        self.data = conn.lrange(key_prefix, 0, -1)?;
        Ok(None)
    }
    
    fn data_len(&self, filtered_data: Option<&[String]>) -> usize {
        if let Some(filtered_data) = filtered_data {
            filtered_data.len()
        } else {
            self.data.len()
        }
    }
    
    fn get_row(&self, index: usize, filtered_data: Option<&[String]>) -> Option<Vec<String>> {
        if let Some(filtered_data) = filtered_data {
            // List data is stored as [element1, element2, ...]
            if index < filtered_data.len() {
                Some(vec![filtered_data[index].clone()])
            } else {
                None
            }
        } else {
            self.data.get(index).map(|item| vec![item.clone()])
        }
    }
    
    fn insert(&mut self, conn: &mut redis::Connection, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        for value in data {
            let _: i32 = conn.rpush(key_prefix, value)?;
            self.data.push(value.clone());
        }
        Ok(())
    }
    
    fn delete(&mut self, conn: &mut redis::Connection, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        for value in data {
            // LREM removes all occurrences of value from the list
            // Using count = 0 to remove all occurrences
            let _: i32 = conn.lrem(key_prefix, 0, value)?;
            // Remove from local data cache
            self.data.retain(|x| x != value);
        }
        Ok(())
    }
    
    fn update(&mut self, conn: &mut redis::Connection, key_prefix: &str, old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        
        // First, remove all old data values
        self.delete(conn, key_prefix, old_data)?;
        
        // Then insert new data values
        self.insert(conn, key_prefix, new_data)?;
        
        Ok(())
    }

    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool {
        matches!(operator, ComparisonOperator::Equal | ComparisonOperator::Like)
    }
}
