use crate::{
    query::pushdown_types::{ComparisonOperator, PushableCondition},
    tables::types::{DataSet, LoadDataResult},
};

/// Trait defining common operations for Redis table types
pub trait RedisTableOperations {
    /// Load data from Redis for scanning operations
    /// If conditions are provided, will attempt to apply pushdown optimizations
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
    ) -> Result<LoadDataResult, redis::RedisError>;

    /// Get the current dataset for this table
    fn get_dataset(&self) -> &DataSet;

    /// Get the number of rows/elements in this table type
    fn data_len(&self) -> usize {
        self.get_dataset().len()
    }

    /// Get a row at the specified index for iteration
    fn get_row(&self, index: usize) -> Option<Vec<String>> {
        self.get_dataset().get_row(index)
    }

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
    fn supports_pushdown(&self, operator: ComparisonOperator) -> bool;
}
