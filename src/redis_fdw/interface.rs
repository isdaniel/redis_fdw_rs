/// Trait defining common operations for Redis table types
pub trait RedisTableOperations {
    /// Load data from Redis for scanning operations
    fn load_data(&mut self, conn: &mut redis::Connection, key_prefix: &str) -> Result<(), redis::RedisError>;
    
    /// Get the number of rows/elements in this table type
    fn data_len(&self) -> usize;
    
    /// Get a row at the specified index for iteration
    fn get_row(&self, index: usize) -> Option<Vec<String>>;
    
    /// Insert data into Redis
    fn insert(&mut self, conn: &mut redis::Connection, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError>;
    
    /// Delete data from Redis
    fn delete(&mut self, conn: &mut redis::Connection, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError>;
    
    /// Update data in Redis
    fn update(&mut self, conn: &mut redis::Connection, key_prefix: &str, old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError>;
}
