use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use crate::redis_fdw::tables::interface::RedisTableOperations;

/// Redis List table type (async version)
#[derive(Debug, Clone)]
pub struct RedisListTable {
    pub data: Vec<String>,
}

impl RedisListTable {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }
}

#[async_trait::async_trait]
impl RedisTableOperations for RedisListTable {
    async fn load_data(&mut self, conn: &mut ConnectionManager, key_prefix: &str) -> Result<(), redis::RedisError> {
        self.data = conn.lrange(key_prefix, 0, -1).await?;
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
            let _: () = conn.rpush(key_prefix, value).await?;
            self.data.push(value.clone());
        }
        Ok(())
    }
    
    async fn delete(&mut self, conn: &mut ConnectionManager, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(value) = data.first() {
            let _: i32 = conn.lrem(key_prefix, 1, value).await?;
            if let Some(pos) = self.data.iter().position(|x| x == value) {
                self.data.remove(pos);
            }
        }
        Ok(())
    }
    
    async fn update(&mut self, conn: &mut ConnectionManager, key_prefix: &str, old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        if let (Some(index_str), Some(new_value)) = (old_data.first(), new_data.first()) {
            if let Ok(index) = index_str.parse::<isize>() {
                let _: () = conn.lset(key_prefix, index, new_value).await?;
                if let Some(item) = self.data.get_mut(index as usize) {
                    *item = new_value.clone();
                }
            }
        }
        Ok(())
    }
}
