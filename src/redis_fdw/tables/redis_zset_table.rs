use redis::Commands;

use crate::redis_fdw::tables::interface::RedisTableOperations;

/// Redis Sorted Set table type
#[derive(Debug, Clone)]
pub struct RedisZSetTable {
    pub data: Vec<(String, f64)>, // (member, score)
}

impl RedisZSetTable {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }
}

impl RedisTableOperations for RedisZSetTable {
    fn load_data(&mut self, conn: &mut redis::Connection, key_prefix: &str) -> Result<(), redis::RedisError> {
        let result: Vec<(String, f64)> = conn.zrange_withscores(key_prefix, 0, -1)?;
        self.data = result;
        Ok(())
    }
    
    fn data_len(&self) -> usize {
        self.data.len()
    }
    
    fn get_row(&self, index: usize) -> Option<Vec<String>> {
        self.data.get(index).map(|(member, score)| vec![member.clone(), score.to_string()])
    }
    
    fn insert(&mut self, conn: &mut redis::Connection, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        // Expect data in pairs: [member1, score1, member2, score2, ...]
        let items: Vec<(f64, String)> = data
            .chunks(2)
            .filter_map(|chunk| {
                if chunk.len() == 2 {
                    if let Ok(score) = chunk[1].parse::<f64>() {
                        Some((score, chunk[0].clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        
        for (score, member) in &items {
            let _ : () = conn.zadd(key_prefix, member, *score)?;
            self.data.push((member.clone(), *score));
        }
        Ok(())
    }
    
    fn delete(&mut self, conn: &mut redis::Connection, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        for member in data {
            let _: i32 = conn.zrem(key_prefix, member)?;
            self.data.retain(|(m, _)| m != member);
        }
        Ok(())
    }
    
    fn update(&mut self, conn: &mut redis::Connection, key_prefix: &str, old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        // For sorted sets, update means remove old members and add new ones
        if !old_data.is_empty() {
            self.delete(conn, key_prefix, old_data)?;
        }
        self.insert(conn, key_prefix, new_data)?;
        Ok(())
    }
}