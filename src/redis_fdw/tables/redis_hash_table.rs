use std::collections::HashMap;
use redis::Commands;

use crate::redis_fdw::tables::interface::RedisTableOperations;

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
    fn load_data(&mut self, conn: &mut redis::Connection, key_prefix: &str) -> Result<(), redis::RedisError> {
        let hash_data: HashMap<String, String> = conn.hgetall(key_prefix)?;
        self.data = hash_data.into_iter().collect();
        Ok(())
    }
    
    fn data_len(&self) -> usize {
        self.data.len()
    }
    
    fn get_row(&self, index: usize) -> Option<Vec<String>> {
        self.data.get(index).map(|(k, v)| vec![k.clone(), v.clone()])
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
}