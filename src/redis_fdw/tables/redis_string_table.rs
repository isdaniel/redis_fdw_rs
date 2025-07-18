use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use crate::redis_fdw::tables::interface::RedisTableOperations;

/// Redis String table type (async version)
#[derive(Debug, Clone)]
pub struct RedisStringTable {
    pub data: Option<String>,
}

impl RedisStringTable {
    pub fn new() -> Self {
        Self { data: None }
    }
}

#[async_trait::async_trait]
impl RedisTableOperations for RedisStringTable {
    async fn load_data(&mut self, conn: &mut ConnectionManager, key_prefix: &str) -> Result<(), redis::RedisError> {
        self.data = conn.get(key_prefix).await?;
        Ok(())
    }
    
    fn data_len(&self) -> usize {
        if self.data.is_some() { 1 } else { 0 }
    }
    
    fn get_row(&self, index: usize) -> Option<Vec<String>> {
        if index == 0 && self.data.is_some() {
            Some(vec![self.data.as_ref().unwrap().clone()])
        } else {
            None
        }
    }
    
    async fn insert(&mut self, conn: &mut ConnectionManager, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(value) = data.first() {
            let _: () = conn.set(key_prefix, value).await?;
            self.data = Some(value.clone());
        }
        Ok(())
    }
    
    async fn delete(&mut self, conn: &mut ConnectionManager, key_prefix: &str, _data: &[String]) -> Result<(), redis::RedisError> {
        let _: () = conn.del(key_prefix).await?;
        self.data = None;
        Ok(())
    }
    
    async fn update(&mut self, conn: &mut ConnectionManager, key_prefix: &str, _old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(value) = new_data.first() {
            let _: () = conn.set(key_prefix, value).await?;
            self.data = Some(value.clone());
        }
        Ok(())
    }
}