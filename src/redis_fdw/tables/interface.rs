use redis::cluster::ClusterConnection;

use crate::redis_fdw::pushdown::{ComparisonOperator, PushableCondition};

/// Trait defining common operations for Redis table types
pub trait RedisTableOperations {
    /// Load data from Redis for scanning operations
    /// If conditions are provided, will attempt to apply pushdown optimizations
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
    ) -> Result<Option<Vec<String>>, redis::RedisError>;

    /// Get the number of rows/elements in this table type
    /// If filtered_data is provided, calculates length from filtered data
    fn data_len(&self, filtered_data: Option<&[String]>) -> usize;

    /// Get a row at the specified index for iteration
    /// If filtered_data is provided, gets row from filtered data instead of internal data
    fn get_row(&self, index: usize, filtered_data: Option<&[String]>) -> Option<Vec<String>>;

    /// Insert data into Redis
    fn insert(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError>;

    /// Delete data from Redis
    fn delete(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError>;

    /// Update data in Redis
    fn update(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        old_data: &[String],
        new_data: &[String],
    ) -> Result<(), redis::RedisError>;

    /// Check if a specific condition can be pushed down for this table type
    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool;
}


/// Enum representing different Redis connection types
pub enum RedisConnectionType {
    Single(redis::Connection),
    Cluster(ClusterConnection),
}

impl RedisConnectionType {
    
    pub fn as_connection_like_mut(&mut self) -> &mut dyn redis::ConnectionLike {
        match self {
            RedisConnectionType::Single(conn) => conn,
            RedisConnectionType::Cluster(conn) => conn,
        }
    }
}