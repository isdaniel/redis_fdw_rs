use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use crate::redis_fdw::tables::interface::RedisTableOperations;
use std::collections::HashMap;

/// Redis Hash table type (async version)
#[derive(Debug, Clone)]
pub struct RedisHashTable {
    pub data: Vec<(String, String)>, // key-value pairs from hash
}

impl RedisHashTable {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }
}

#[async_trait::async_trait]
impl RedisTableOperations for RedisHashTable {
    async fn load_data(&mut self, conn: &mut ConnectionManager, key_prefix: &str) -> Result<(), redis::RedisError> {
        let hash_data: HashMap<String, String> = conn.hgetall(key_prefix).await?;
        self.data = hash_data.into_iter().collect();
        Ok(())
    }
    
    fn data_len(&self) -> usize {
        self.data.len()
    }
    
    fn get_row(&self, index: usize) -> Option<Vec<String>> {
        self.data.get(index).map(|(k, v)| vec![k.clone(), v.clone()])
    }
    
    async fn insert(&mut self, conn: &mut ConnectionManager, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        if data.len() >= 2 {
            let field = &data[0];
            let value = &data[1];
            let _: () = conn.hset(key_prefix, field, value).await?;
            self.data.push((field.clone(), value.clone()));
        }
        Ok(())
    }
    
    async fn delete(&mut self, conn: &mut ConnectionManager, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(field) = data.first() {
            let _: () = conn.hdel(key_prefix, field).await?;
            self.data.retain(|(k, _)| k != field);
        }
        Ok(())
    }
    
    async fn update(&mut self, conn: &mut ConnectionManager, key_prefix: &str, _old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        if new_data.len() >= 2 {
            let field = &new_data[0];
            let value = &new_data[1];
            let _: () = conn.hset(key_prefix, field, value).await?;
            
            // Update local data
            if let Some(pos) = self.data.iter().position(|(k, _)| k == field) {
                self.data[pos].1 = value.clone();
            } else {
                self.data.push((field.clone(), value.clone()));
            }
        }
        Ok(())
    }
}