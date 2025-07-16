use redis::Commands;

use crate::redis_fdw::interface::RedisTableOperations;

/// Redis Set table type
#[derive(Debug, Clone)]
pub struct RedisSetTable {
    pub data: Vec<String>,
}

impl RedisSetTable {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }
}

impl RedisTableOperations for RedisSetTable {
    fn load_data(&mut self, conn: &mut redis::Connection, key_prefix: &str) -> Result<(), redis::RedisError> {
        self.data = conn.smembers( key_prefix)?;
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
            let added: i32 = conn.sadd(key_prefix, value)?;
            if added > 0 {
                self.data.push(value.clone());
            }
        }
        Ok(())
    }
    
    fn delete(&mut self, conn: &mut redis::Connection, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        for value in data {
            let _: i32 = conn.srem( key_prefix, value)?;
            self.data.retain(|x| x != value);
        }
        Ok(())
    }
    
    fn update(&mut self, conn: &mut redis::Connection, key_prefix: &str, old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        // For sets, update means remove old and add new
        self.delete(conn, key_prefix, old_data)?;
        self.insert(conn, key_prefix, new_data)?;
        Ok(())
    }
}