use crate::redis_fdw::tables::interface::RedisTableOperations;
use redis::AsyncCommands;
use async_trait::async_trait;

/// Redis List table type
#[derive(Debug, Clone)]
pub struct RedisListTable {
    pub data: Vec<String>,
}

impl RedisListTable {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }
}

#[async_trait]
impl RedisTableOperations for RedisListTable {
    
    /// Load data from Redis list
    /// This method retrieves all elements from the Redis list at the specified key prefix.
    async fn load_data(&mut self, conn: &mut redis::aio::ConnectionManager, key_prefix: &str) -> Result<(), redis::RedisError> {
        self.data = conn.lrange(key_prefix, 0, -1).await?;
        Ok(())
    }
    
    fn data_len(&self) -> usize {
        self.data.len()
    }
    
    fn get_row(&self, index: usize) -> Option<Vec<String>> {
        self.data.get(index).map(|item| vec![item.clone()])
    }
    
    async fn insert(&mut self, conn: &mut redis::aio::ConnectionManager, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        for value in data {
            let _: i32 = conn.rpush(key_prefix, value).await?;
            self.data.push(value.clone());
        }
        Ok(())
    }
    
    async fn delete(&mut self, conn: &mut redis::aio::ConnectionManager, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        for value in data {
            // LREM removes all occurrences of value from the list
            // Using count = 0 to remove all occurrences
            let _: i32 = conn.lrem(key_prefix, 0, value).await?;
            // Remove from local data cache
            self.data.retain(|x| x != value);
        }
        Ok(())
    }
    
    async fn update(&mut self, conn: &mut redis::aio::ConnectionManager, key_prefix: &str, old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        
        // First, remove all old data values
        self.delete(conn, key_prefix, old_data).await?;
        
        // Then insert new data values
        self.insert(conn, key_prefix, new_data).await?;
        
        Ok(())
    }
}
