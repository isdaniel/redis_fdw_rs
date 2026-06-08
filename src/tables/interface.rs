use crate::{
    query::{
        limit::LimitOffsetInfo,
        pushdown_types::{ComparisonOperator, PushableCondition},
    },
    tables::types::{DataSet, LoadDataResult, RowVec},
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

    /// Get the number of rows/elements in this table type
    #[inline]
    fn data_len(&self) -> usize {
        self.get_dataset().len()
    }

    /// Get a row at the specified index for iteration - returns borrowed strings to avoid cloning
    #[inline]
    fn get_row(&self, index: usize) -> Option<RowVec<'_>> {
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

    /// Configure type-specific state after column information is known.
    fn configure(
        &mut self,
        _column_names: &[String],
        _pushdown_column_index: usize,
        _score_column_index: Option<usize>,
    ) {
    }

    /// Load data for multiple keys in multi-key mode.
    /// Returns flat Vec<String> with [key, col1, col2, ...] repeated per row.
    fn load_multi_key_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        keys: &[String],
    ) -> Result<Vec<String>, redis::RedisError>;

    /// Reset internal dataset state for rescan.
    fn clear(&mut self);

    /// Redis TYPE name for SCAN TYPE filter (e.g., "hash", "string").
    fn redis_type_name(&self) -> &'static str;

    /// Set flat multi-key filtered data directly.
    fn set_filtered_data(&mut self, data: Vec<String>);

    /// Get number of columns per row in multi-key flat format.
    fn multi_key_columns_per_row(&self) -> usize {
        2
    }

    /// Batched parameterized lookup. Called by `RedisFdwState` during
    /// nested-loop joins to amortize per-row Redis round-trips.
    ///
    /// `key_prefix` is the table's `table_key_prefix` (for hash/set/zset it's
    /// the Redis key; for string in multi-key mode `key_prefix` is unused and
    /// `params` are the keys themselves).
    ///
    /// Default impl returns `Ok` with `None` for every param; per-type impls
    /// override with pipelined commands (HMGET/MGET/pipelined SISMEMBER/ZSCORE).
    /// `RedisFdwState` routes the per-key fallback explicitly when a type
    /// opts out — the trait does not call back into the single-lookup path.
    ///
    /// Returns `Ok(Vec<Option<Vec<String>>>)` of the same length as `params`:
    /// each element is `Some(row)` on hit, `None` on miss. Row layout matches
    /// the single-row dataset layout for that table type. On Redis error,
    /// returns `Err` so the caller can decide whether to abort the transaction
    /// at the FDW boundary (preferable to invoking `pgrx::error!` here, which
    /// would longjmp past Rust destructors).
    fn batch_parameterized_lookup(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        params: &[String],
    ) -> Result<Vec<Option<Vec<String>>>, redis::RedisError> {
        let _ = (conn, key_prefix);
        Ok(vec![None; params.len()])
    }
}

#[cfg(test)]
mod batch_lookup_trait_tests {
    use super::*;
    use redis::{ConnectionLike, RedisResult, Value};

    /// Minimal RedisTableOperations stub. Used solely to verify the default
    /// implementation of `batch_parameterized_lookup` returns one `None` per
    /// param without consulting the connection — all other methods panic
    /// (unreachable) because the test only exercises the default impl.
    #[derive(Default)]
    struct StubTable {
        dataset: DataSet,
    }

    impl RedisTableOperations for StubTable {
        fn load_data(
            &mut self,
            _conn: &mut dyn redis::ConnectionLike,
            _key_prefix: &str,
            _conditions: Option<&[PushableCondition]>,
            _limit_offset: &LimitOffsetInfo,
        ) -> Result<LoadDataResult, redis::RedisError> {
            unreachable!("stub")
        }
        fn load_batch(
            &mut self,
            _conn: &mut dyn redis::ConnectionLike,
            _key_prefix: &str,
            _cursor: u64,
            _batch_size: usize,
            _conditions: Option<&[PushableCondition]>,
        ) -> Result<(u64, usize), redis::RedisError> {
            unreachable!("stub")
        }
        fn get_dataset(&self) -> &DataSet {
            &self.dataset
        }
        fn insert(
            &mut self,
            _conn: &mut dyn redis::ConnectionLike,
            _key_prefix: &str,
            _data: &[String],
        ) -> Result<(), redis::RedisError> {
            unreachable!("stub")
        }
        fn delete(
            &mut self,
            _conn: &mut dyn redis::ConnectionLike,
            _key_prefix: &str,
            _data: &[String],
        ) -> Result<(), redis::RedisError> {
            unreachable!("stub")
        }
        fn update(
            &mut self,
            _conn: &mut dyn redis::ConnectionLike,
            _key_prefix: &str,
            _old_data: &[String],
            _new_data: &[String],
        ) -> Result<(), redis::RedisError> {
            unreachable!("stub")
        }
        fn supports_pushdown(&self, _op: &ComparisonOperator) -> bool {
            false
        }
        fn load_multi_key_data(
            &mut self,
            _conn: &mut dyn redis::ConnectionLike,
            _keys: &[String],
        ) -> Result<Vec<String>, redis::RedisError> {
            unreachable!("stub")
        }
        fn clear(&mut self) {}
        fn redis_type_name(&self) -> &'static str {
            "stub"
        }
        fn set_filtered_data(&mut self, _data: Vec<String>) {}
        // batch_parameterized_lookup intentionally NOT overridden — that's the SUT.
    }

    /// ConnectionLike that panics on any wire call. The default
    /// `batch_parameterized_lookup` must NOT touch the connection.
    struct PanicConn;
    impl ConnectionLike for PanicConn {
        fn req_packed_command(&mut self, _cmd: &[u8]) -> RedisResult<Value> {
            panic!("default batch_parameterized_lookup must not issue Redis commands");
        }
        fn req_packed_commands(
            &mut self,
            _cmd: &[u8],
            _offset: usize,
            _count: usize,
        ) -> RedisResult<Vec<Value>> {
            panic!("default batch_parameterized_lookup must not issue Redis commands");
        }
        fn get_db(&self) -> i64 {
            0
        }
        fn check_connection(&mut self) -> bool {
            true
        }
        fn is_open(&self) -> bool {
            true
        }
    }

    #[test]
    fn default_batch_returns_none_per_param_without_touching_connection() {
        let mut t = StubTable::default();
        let mut conn = PanicConn;
        let params = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let result = t
            .batch_parameterized_lookup(&mut conn, "ignored:prefix", &params)
            .expect("default impl is infallible");
        assert_eq!(result.len(), params.len());
        assert!(result.iter().all(|r| r.is_none()));
    }

    #[test]
    fn default_batch_empty_params_returns_empty_vec() {
        let mut t = StubTable::default();
        let mut conn = PanicConn;
        let result = t
            .batch_parameterized_lookup(&mut conn, "", &[])
            .expect("default impl is infallible");
        assert!(result.is_empty());
    }
}
