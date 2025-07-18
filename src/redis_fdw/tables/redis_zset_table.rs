use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use crate::redis_fdw::tables::interface::RedisTableOperations;

/// Redis Sorted Set (ZSet) table type (async version)
#[derive(Debug, Clone)]
pub struct RedisZSetTable {
    pub data: Vec<(String, f64)>, // (member, score) pairs
}

impl RedisZSetTable {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }
}

#[async_trait::async_trait]
impl RedisTableOperations for RedisZSetTable {
    async fn load_data(&mut self, conn: &mut ConnectionManager, key_prefix: &str) -> Result<(), redis::RedisError> {
        let zset_data: Vec<(String, f64)> = conn.zrange_withscores(key_prefix, 0, -1).await?;
        self.data = zset_data;
        Ok(())
    }
    
    fn data_len(&self) -> usize {
        self.data.len()
    }
    
    fn get_row(&self, index: usize) -> Option<Vec<String>> {
        self.data.get(index).map(|(member, score)| vec![member.clone(), score.to_string()])
    }
    
    async fn insert(&mut self, conn: &mut ConnectionManager, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        if data.len() >= 2 {
            if let (Some(member), Ok(score)) = (data.first(), data[1].parse::<f64>()) {
                let _: () = conn.zadd(key_prefix, member, score).await?;
                
                // Update local data - remove existing member if present, then add
                self.data.retain(|(m, _)| m != member);
                self.data.push((member.clone(), score));
                self.data.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            }
        }
        Ok(())
    }
    
    async fn delete(&mut self, conn: &mut ConnectionManager, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(member) = data.first() {
            let _: i32 = conn.zrem(key_prefix, member).await?;
            self.data.retain(|(m, _)| m != member);
        }
        Ok(())
    }
    
    async fn update(&mut self, conn: &mut ConnectionManager, key_prefix: &str, _old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        if new_data.len() >= 2 {
            if let (Some(member), Ok(score)) = (new_data.first(), new_data[1].parse::<f64>()) {
                let _: () = conn.zadd(key_prefix, member, score).await?;
                
                // Update local data
                if let Some(pos) = self.data.iter().position(|(m, _)| m == member) {
                    self.data[pos].1 = score;
                } else {
                    self.data.push((member.clone(), score));
                }
                self.data.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            }
        }
        Ok(())
    }
}