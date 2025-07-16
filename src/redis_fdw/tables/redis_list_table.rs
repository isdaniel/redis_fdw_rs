use crate::redis_fdw::tables::interface::RedisTableOperations;
use redis::Commands;

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

impl RedisTableOperations for RedisListTable {
    fn load_data(&mut self, conn: &mut redis::Connection, key_prefix: &str) -> Result<(), redis::RedisError> {
        self.data = conn.lrange(key_prefix, 0, -1)?;
        Ok(())
    }
    
    fn data_len(&self) -> usize {
        self.data.len()
    }
    
    fn get_row(&self, index: usize) -> Option<Vec<String>> {
        self.data.get(index).map(|item| vec![item.clone()])
    }
    
    fn insert(&mut self, conn: &mut redis::Connection, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        for value in data {
            let _: i32 = conn.rpush(key_prefix, value)?;
            self.data.push(value.clone());
        }
        Ok(())
    }
    
    fn delete(&mut self, conn: &mut redis::Connection, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        unimplemented!("Update operation for Redis List is not defined in this context");
    }
    
    fn update(&mut self, conn: &mut redis::Connection, key_prefix: &str, old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        unimplemented!("Update operation for Redis List is not defined in this context");
    }
}
