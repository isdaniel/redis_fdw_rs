use crate::{
    query::{
        limit::LimitOffsetInfo,
        pushdown_types::{ComparisonOperator, PushableCondition},
    },
    tables::types::{DataSet, LoadDataResult},
};
use std::borrow::Cow;

/// Trait defining common operations for Redis table types
#[allow(dead_code)]
pub trait RedisTableOperations {
    /// Load data from Redis for scanning operations
    /// If conditions are provided, will attempt to apply pushdown optimizations
    fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
        limit_offset: &LimitOffsetInfo,
    ) -> Result<LoadDataResult, redis::RedisError>;

    /// Load a batch of data using cursor-based iteration for streaming.
    /// Returns (new_cursor, rows_loaded). When new_cursor == 0, iteration is complete.
    fn load_batch(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        cursor: u64,
        batch_size: usize,
        conditions: Option<&[PushableCondition]>,
    ) -> Result<(u64, usize), redis::RedisError>;

    /// Get the current dataset for this table
    fn get_dataset(&self) -> &DataSet;

    /// Get a mutable reference to the dataset for this table
    fn get_dataset_mut(&mut self) -> &mut DataSet;

    /// Get the number of rows/elements in this table type
    #[inline]
    fn data_len(&self) -> usize {
        self.get_dataset().len()
    }

    /// Get a row at the specified index for iteration - returns borrowed strings to avoid cloning
    #[inline]
    fn get_row(&self, index: usize) -> Option<Vec<Cow<'_, str>>> {
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

    /// Update data in Redis (old values -> new values)
    fn update(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        old_data: &[String],
        new_data: &[String],
    ) -> Result<(), redis::RedisError>;

    /// Check if a specific condition can be pushed down for this table type
    fn supports_pushdown(&self, operator: &ComparisonOperator) -> bool;

    /// Set filtered data directly (used when external filtering is applied)
    fn set_filtered_data(&mut self, data: Vec<String>) {
        *self.get_dataset_mut() = DataSet::Filtered(data);
    }
}
