use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use crate::redis_fdw::tables::interface::RedisTableOperations;
use std::collections::HashSet;

/// Redis Set table type (async version)
#[derive(Debug, Clone)]
pub struct RedisSetTable {
    pub data: Vec<String>,
}

impl RedisSetTable {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }
}

#[async_trait::async_trait]
impl RedisTableOperations for RedisSetTable {
    async fn load_data(&mut self, conn: &mut ConnectionManager, key_prefix: &str) -> Result<(), redis::RedisError> {
        let set_data: HashSet<String> = conn.smembers(key_prefix).await?;
        self.data = set_data.into_iter().collect();
        Ok(())
    }
    
    fn data_len(&self) -> usize {
        self.data.len()
    }
    
    fn get_row(&self, index: usize) -> Option<Vec<String>> {
        self.data.get(index).map(|v| vec![v.clone()])
    }
    
    async fn insert(&mut self, conn: &mut ConnectionManager, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(value) = data.first() {
            let _: () = conn.sadd(key_prefix, value).await?;
            if !self.data.contains(value) {
                self.data.push(value.clone());
            }
        }
        Ok(())
    }
    
    async fn delete(&mut self, conn: &mut ConnectionManager, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(value) = data.first() {
            let _: i32 = conn.srem(key_prefix, value).await?;
            self.data.retain(|x| x != value);
        }
        Ok(())
    }
    
    async fn update(&mut self, conn: &mut ConnectionManager, key_prefix: &str, old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        // For sets, update means remove old value and add new value
        if let (Some(old_value), Some(new_value)) = (old_data.first(), new_data.first()) {
            let _: i32 = conn.srem(key_prefix, old_value).await?;
            let _: () = conn.sadd(key_prefix, new_value).await?;
            
            if let Some(pos) = self.data.iter().position(|x| x == old_value) {
                self.data[pos] = new_value.clone();
            }
        }
        Ok(())
    }
}