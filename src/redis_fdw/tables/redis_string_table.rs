use redis::Commands;

use crate::redis_fdw::tables::interface::RedisTableOperations;
/// Redis String table type
#[derive(Debug, Clone)]
pub struct RedisStringTable {
    pub data: Option<String>,
}

impl RedisStringTable {
    pub fn new() -> Self {
        Self { data: None }
    }
}

impl RedisTableOperations for RedisStringTable {
    fn load_data(&mut self, conn: &mut redis::Connection, key_prefix: &str) -> Result<(), redis::RedisError> {
        self.data = conn.get( key_prefix)?;
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
    
    fn insert(&mut self, conn: &mut redis::Connection, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(value) = data.first() {
            let _ : () = conn.set(key_prefix, value)?;
            self.data = Some(value.clone());
        }
        Ok(())
    }
    
    fn delete(&mut self, conn: &mut redis::Connection, key_prefix: &str, _data: &[String]) -> Result<(), redis::RedisError> {
        let _ : () = conn.del(key_prefix)?;
        self.data = None;
        Ok(())
    }
    
    fn update(&mut self, conn: &mut redis::Connection, key_prefix: &str, _old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(value) = new_data.first() {
            let _ : () = conn.set( key_prefix, value)?;
            self.data = Some(value.clone());
        }
        Ok(())
    }
}