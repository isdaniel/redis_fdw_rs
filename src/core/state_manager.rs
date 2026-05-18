/// State management module for Redis FDW
///
/// This module contains simplified state management focused on
/// configuration, connection status, and coordination between components.
use crate::{
    core::{
        connection_factory::{RedisConnectionConfig, RedisConnectionFactory},
        pool_manager::PooledConnection,
    },
    query::{
        cost_estimation::{CostEstimate, CostEstimator},
        pushdown_types::{ComparisonOperator, PushdownAnalysis},
    },
    tables::types::RedisTableType,
};
use pgrx::{pg_sys::MemoryContext, prelude::*};
use std::borrow::Cow;
use std::collections::HashMap;

/// Simplified Redis FDW state focused on state management
pub struct RedisFdwState {
    pub tmp_ctx: MemoryContext,
    pub redis_connection: Option<PooledConnection>,
    pub database: i64,
    pub host_port: String,
    pub table_type: RedisTableType,
    pub table_key_prefix: String,
    pub opts: HashMap<String, String>,
    pub row_count: u32,
    pub key_attno: i16,
    pub pushdown_analysis: Option<PushdownAnalysis>,
    /// Cached cost estimate for query planning
    pub cost_estimate: Option<CostEstimate>,
    /// Streaming state: Redis SCAN cursor position (0 = start, returned 0 = done)
    pub scan_cursor: u64,
    /// Whether we've completed the full scan (cursor returned 0)
    pub scan_complete: bool,
    /// Batch size for streaming (configurable via table option)
    pub batch_size: usize,
    /// Column index of the `ttl` column in the tuple descriptor (None = no ttl column)
    pub ttl_column_index: Option<usize>,
    /// Default TTL from table OPTIONS (None = no default)
    pub default_ttl: Option<i64>,
    /// Whether this table operates in multi-key mode (glob pattern in table_key_prefix)
    pub is_multi_key: bool,
    /// Cached TTL value for single-key mode (avoids repeated TTL calls per row)
    pub cached_ttl: Option<i64>,
}

impl RedisFdwState {
    pub fn new(tmp_ctx: MemoryContext) -> Self {
        RedisFdwState {
            tmp_ctx,
            redis_connection: None,
            table_type: RedisTableType::None,
            table_key_prefix: String::default(),
            database: 0,
            host_port: String::default(),
            opts: HashMap::default(),
            row_count: 0,
            key_attno: 0,
            pushdown_analysis: None,
            cost_estimate: None,
            scan_cursor: 0,
            scan_complete: false,
            batch_size: 1000,
            ttl_column_index: None,
            default_ttl: None,
            is_multi_key: false,
            cached_ttl: None,
        }
    }

    /// Initialize Redis connection using the connection factory with authentication
    /// Returns Result for proper error handling instead of panicking
    pub fn init_redis_connection_from_options(&mut self) -> Result<(), String> {
        let config = RedisConnectionConfig::from_options(&self.opts)
            .map_err(|e| format!("Failed to create Redis configuration: {}", e))?;

        // Create connection using the factory with retry logic
        match RedisConnectionFactory::create_connection_with_retry(&config) {
            Ok(connection) => {
                self.redis_connection = Some(connection);
                log!("Successfully initialized Redis connection with authentication");
                Ok(())
            }
            Err(e) => {
                let error_msg = format!("Failed to initialize Redis connection: {}", e);
                Err(error_msg)
            }
        }
    }

    /// Updates the struct fields from a HashMap
    pub fn update_from_options(&mut self, opts: HashMap<String, String>) {
        self.opts = opts;

        self.host_port = self
            .opts
            .get("host_port")
            .expect("`host_port` option is required for redis_fdw")
            .clone();

        if let Some(db_str) = self.opts.get("database") {
            self.database = db_str
                .parse::<i64>()
                .unwrap_or_else(|_| panic!("Invalid `database` value: {db_str}"));
        }

        if let Some(prefix) = self.opts.get("table_key_prefix") {
            self.table_key_prefix = prefix.clone();
        }

        if let Some(bs) = self.opts.get("batch_size") {
            if let Ok(size) = bs.parse::<usize>() {
                self.batch_size = size.clamp(100, 100_000);
            }
        }

        if let Some(ttl_str) = self.opts.get("ttl") {
            if let Ok(ttl) = ttl_str.parse::<i64>() {
                if ttl > 0 || ttl == -1 {
                    self.default_ttl = Some(ttl);
                }
            }
        }

        self.is_multi_key = is_multi_key_pattern(&self.table_key_prefix);
    }

    /// Set table type and prepare for streaming iteration
    pub fn set_table_type(&mut self) {
        let table_type = self
            .opts
            .get("table_type")
            .expect("`table_type` option is required for redis_fdw");

        self.table_type = RedisTableType::from_str(table_type);
    }

    /// Fetch the next batch of data using cursor-based iteration.
    /// Returns true if more data was loaded, false if scan is complete.
    /// Note: Redis SCAN can return 0 elements with a non-zero cursor, so we must loop until we get data or the cursor returns to 0.
    pub fn fetch_next_batch(&mut self) -> bool {
        if self.scan_complete {
            return false;
        }

        if self.is_multi_key {
            return self.fetch_next_batch_multi_key();
        }

        // Determine strategy before borrowing connection
        let use_direct_load = self.scan_cursor == 0 && self.should_use_direct_load();

        if let Some(ref mut conn) = self.redis_connection {
            let conn_like = conn.as_connection_like_mut();

            // On first call, use optimized direct-load path when conditions support it
            // (Equal → HGET/SISMEMBER/ZSCORE, In → HMGET/SMISMEMBER, Like+LIMIT → SCAN with limit)
            if use_direct_load {
                let conditions = self
                    .pushdown_analysis
                    .as_ref()
                    .map(|a| a.pushable_conditions.as_slice());
                let limit_offset = self
                    .pushdown_analysis
                    .as_ref()
                    .and_then(|a| a.limit_offset.clone())
                    .unwrap_or_default();

                match self.table_type.load_data(
                    conn_like,
                    &self.table_key_prefix,
                    conditions,
                    &limit_offset,
                ) {
                    Ok(_) => {
                        self.scan_complete = true;
                        return self.table_type.data_len() > 0;
                    }
                    Err(e) => {
                        pgrx::error!("Redis error during optimized data load: {}", e);
                    }
                }
            }

            // Streaming path: cursor-based SCAN for large datasets without direct-lookup conditions
            let conditions = self.pushdown_analysis.as_ref().and_then(|a| {
                if a.has_optimizations() {
                    Some(a.pushable_conditions.as_slice())
                } else {
                    None
                }
            });

            loop {
                pgrx::check_for_interrupts!();
                match self.table_type.load_batch(
                    conn_like,
                    &self.table_key_prefix,
                    self.scan_cursor,
                    self.batch_size,
                    conditions,
                ) {
                    Ok((new_cursor, rows_loaded)) => {
                        self.scan_cursor = new_cursor;
                        if new_cursor == 0 {
                            self.scan_complete = true;
                        }
                        if rows_loaded > 0 {
                            return true;
                        }
                        if self.scan_complete {
                            return false;
                        }
                    }
                    Err(e) => {
                        pgrx::error!("Redis error during batch fetch: {}", e);
                    }
                }
            }
        } else {
            self.scan_complete = true;
            false
        }
    }

    /// Determine whether to use the optimized `load_data` path instead of streaming `load_batch`.
    /// Returns true when conditions can leverage direct Redis commands (HGET, HMGET, SISMEMBER, etc.)
    /// which are O(1) or O(K) instead of O(N) cursor-based scanning.
    fn should_use_direct_load(&self) -> bool {
        if let Some(ref analysis) = self.pushdown_analysis {
            if !analysis.has_optimizations() {
                return false;
            }

            // Use direct load if any condition is Equal or In (direct lookup commands)
            let has_direct_lookup = analysis.pushable_conditions.iter().any(|c| {
                matches!(
                    c.operator,
                    ComparisonOperator::Equal | ComparisonOperator::In
                )
            });
            if has_direct_lookup {
                return true;
            }

            // Use direct load if we have LIMIT/OFFSET (load_data handles it efficiently)
            if analysis.has_limit_pushdown() {
                return true;
            }
        }
        false
    }

    /// Set pushdown analysis from planner
    pub fn set_pushdown_analysis(&mut self, analysis: PushdownAnalysis) {
        log!(
            "Setting pushdown analysis: can_optimize={}, conditions={:?}",
            analysis.can_optimize,
            analysis.pushable_conditions
        );
        self.pushdown_analysis = Some(analysis);
    }

    /// Check if we've read all available data
    pub fn is_read_end(&self) -> bool {
        self.row_count >= self.data_len() as u32
    }

    /// Get the total number of data items
    pub fn data_len(&self) -> usize {
        if self.is_multi_key {
            let cols = self.multi_key_columns_per_row();
            if cols == 0 {
                return 0;
            }
            if let Some(filtered) = self.table_type.get_dataset_ref().as_filtered() {
                return filtered.len() / cols;
            }
            0
        } else {
            self.table_type.data_len()
        }
    }

    /// Insert data using the appropriate table type
    pub fn insert_data(&mut self, data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(conn) = self.redis_connection.as_mut() {
            let conn_like = conn.as_connection_like_mut();
            self.table_type
                .insert(conn_like, &self.table_key_prefix, data)
        } else {
            Err(redis::RedisError::from((
                redis::ErrorKind::Io,
                "Redis connection not initialized",
            )))
        }
    }

    /// Delete data using the appropriate table type
    pub fn delete_data(&mut self, data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(conn) = self.redis_connection.as_mut() {
            let conn_like = conn.as_connection_like_mut();
            self.table_type
                .delete(conn_like, &self.table_key_prefix, data)
        } else {
            Err(redis::RedisError::from((
                redis::ErrorKind::Io,
                "Redis connection not initialized",
            )))
        }
    }

    /// Update data using the appropriate table type
    pub fn update_data(
        &mut self,
        old_data: &[String],
        new_data: &[String],
    ) -> Result<(), redis::RedisError> {
        if let Some(conn) = self.redis_connection.as_mut() {
            let conn_like = conn.as_connection_like_mut();
            self.table_type
                .update(conn_like, &self.table_key_prefix, old_data, new_data)
        } else {
            Err(redis::RedisError::from((
                redis::ErrorKind::Io,
                "Redis connection not initialized",
            )))
        }
    }

    /// Get a row at the specified index
    #[inline]
    pub fn get_row(&self, index: usize) -> Option<Vec<Cow<'_, str>>> {
        self.table_type.get_row(index)
    }

    /// Estimate the cost for scanning this foreign relation
    ///
    /// This method gathers statistics from Redis and calculates appropriate
    /// cost estimates for the PostgreSQL query planner.
    pub fn estimate_costs(&mut self) -> CostEstimate {
        let estimator = CostEstimator::new(
            &self.table_type,
            &self.table_key_prefix,
            self.pushdown_analysis.as_ref(),
        );

        if let Some(ref mut conn) = self.redis_connection {
            let conn_like = conn.as_connection_like_mut();
            let stats = estimator.gather_statistics(conn_like);
            log!(
                "Gathered Redis statistics: db_keys={:?}, key_cardinality={:?}, matching_keys={:?}",
                stats.db_key_count,
                stats.key_cardinality,
                stats.matching_key_count
            );
            estimator.calculate_cost(&stats)
        } else {
            log!("No Redis connection available, using default estimates");
            estimator.estimate_without_connection()
        }
    }

    /// Fetch next batch in multi-key mode using top-level SCAN.
    fn fetch_next_batch_multi_key(&mut self) -> bool {
        // Take connection out temporarily to avoid borrow conflicts
        let mut conn = match self.redis_connection.take() {
            Some(c) => c,
            None => {
                self.scan_complete = true;
                return false;
            }
        };

        let result = self.fetch_multi_key_with_conn(conn.as_connection_like_mut());

        // Put connection back
        self.redis_connection = Some(conn);
        result
    }

    fn fetch_multi_key_with_conn(&mut self, conn: &mut dyn redis::ConnectionLike) -> bool {
        loop {
            pgrx::check_for_interrupts!();

            let mut cmd = redis::cmd("SCAN");
            cmd.arg(self.scan_cursor)
                .arg("MATCH")
                .arg(&self.table_key_prefix)
                .arg("COUNT")
                .arg(self.batch_size);

            let (new_cursor, keys): (u64, Vec<String>) = match cmd.query(conn) {
                Ok(result) => result,
                Err(e) => {
                    pgrx::error!("Redis error during multi-key SCAN: {}", e);
                }
            };

            self.scan_cursor = new_cursor;
            if new_cursor == 0 {
                self.scan_complete = true;
            }

            if keys.is_empty() {
                if self.scan_complete {
                    return false;
                }
                continue;
            }

            let rows = self.load_multi_key_data(conn, &keys);
            if rows > 0 {
                return true;
            }

            if self.scan_complete {
                return false;
            }
        }
    }

    /// Load data for multiple keys and store as flat filtered data.
    fn load_multi_key_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        keys: &[String],
    ) -> usize {
        let mut all_rows: Vec<String> = Vec::new();

        match &self.table_type {
            RedisTableType::String(_) => {
                let values: Vec<Option<String>> = match redis::cmd("MGET").arg(keys).query(conn) {
                    Ok(v) => v,
                    Err(e) => {
                        log!("WARNING: Redis MGET error: {}", e);
                        return 0;
                    }
                };
                for (key, value) in keys.iter().zip(values) {
                    if let Some(v) = value {
                        all_rows.push(key.clone());
                        all_rows.push(v);
                    }
                }
            }
            RedisTableType::Hash(_) => {
                for key in keys {
                    let pairs: Vec<(String, String)> =
                        match redis::cmd("HGETALL").arg(key).query(conn) {
                            Ok(v) => v,
                            Err(e) => {
                                log!("WARNING: Redis HGETALL error for key '{}': {}", key, e);
                                continue;
                            }
                        };
                    for (field, value) in pairs {
                        all_rows.push(key.clone());
                        all_rows.push(field);
                        all_rows.push(value);
                    }
                }
            }
            RedisTableType::List(_) => {
                for key in keys {
                    let items: Vec<String> = match redis::cmd("LRANGE")
                        .arg(key)
                        .arg(0i64)
                        .arg(-1i64)
                        .query(conn)
                    {
                        Ok(v) => v,
                        Err(e) => {
                            log!("WARNING: Redis LRANGE error for key '{}': {}", key, e);
                            continue;
                        }
                    };
                    for item in items {
                        all_rows.push(key.clone());
                        all_rows.push(item);
                    }
                }
            }
            RedisTableType::Set(_) => {
                for key in keys {
                    let members: Vec<String> = match redis::cmd("SMEMBERS").arg(key).query(conn) {
                        Ok(v) => v,
                        Err(e) => {
                            log!("WARNING: Redis SMEMBERS error for key '{}': {}", key, e);
                            continue;
                        }
                    };
                    for member in members {
                        all_rows.push(key.clone());
                        all_rows.push(member);
                    }
                }
            }
            RedisTableType::ZSet(_) => {
                for key in keys {
                    let items: Vec<(String, f64)> = match redis::cmd("ZRANGE")
                        .arg(key)
                        .arg(0i64)
                        .arg(-1i64)
                        .arg("WITHSCORES")
                        .query(conn)
                    {
                        Ok(v) => v,
                        Err(e) => {
                            log!("WARNING: Redis ZRANGE error for key '{}': {}", key, e);
                            continue;
                        }
                    };
                    for (member, score) in items {
                        all_rows.push(key.clone());
                        all_rows.push(score.to_string());
                        all_rows.push(member);
                    }
                }
            }
            RedisTableType::Stream(_) | RedisTableType::None => {}
        }

        let cols_per_row = self.multi_key_columns_per_row();
        let row_count = all_rows.len().checked_div(cols_per_row).unwrap_or(0);

        if !all_rows.is_empty() {
            self.table_type.set_multi_key_data(all_rows);
        }
        row_count
    }

    /// Number of columns per row in multi-key mode (including the key column)
    pub fn multi_key_columns_per_row(&self) -> usize {
        match &self.table_type {
            RedisTableType::String(_) => 2,
            RedisTableType::Hash(_) => 3,
            RedisTableType::List(_) => 2,
            RedisTableType::Set(_) => 2,
            RedisTableType::ZSet(_) => 3,
            RedisTableType::Stream(_) => 4,
            RedisTableType::None => 0,
        }
    }

    /// Apply TTL to a Redis key based on per-row value or table default.
    pub fn apply_ttl(&mut self, key: &str, row_ttl: Option<i64>) {
        let effective_ttl = match row_ttl {
            Some(0) => return,
            Some(t) => t,
            None => match self.default_ttl {
                Some(t) => t,
                None => return,
            },
        };

        if let Some(ref mut conn) = self.redis_connection {
            let conn_like = conn.as_connection_like_mut();
            if effective_ttl > 0 {
                if let Err(e) = redis::cmd("EXPIRE")
                    .arg(key)
                    .arg(effective_ttl)
                    .query::<()>(conn_like)
                {
                    log!("WARNING: Failed to set EXPIRE on key '{}': {}", key, e);
                }
            } else if effective_ttl == -1 {
                if let Err(e) = redis::cmd("PERSIST").arg(key).query::<()>(conn_like) {
                    log!("WARNING: Failed to PERSIST key '{}': {}", key, e);
                }
            }
        }
    }

    /// Read the current TTL for a key. Caches in single-key mode.
    pub fn read_ttl(&mut self, key: &str) -> i64 {
        if !self.is_multi_key {
            if let Some(cached) = self.cached_ttl {
                return cached;
            }
        }

        let ttl = if let Some(ref mut conn) = self.redis_connection {
            let conn_like = conn.as_connection_like_mut();
            redis::cmd("TTL")
                .arg(key)
                .query::<i64>(conn_like)
                .unwrap_or(-2)
        } else {
            -2
        };

        if !self.is_multi_key {
            self.cached_ttl = Some(ttl);
        }
        ttl
    }

    /// Insert data to a specific key (used in multi-key mode)
    pub fn insert_data_to_key(
        &mut self,
        key: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        if let Some(conn) = self.redis_connection.as_mut() {
            let conn_like = conn.as_connection_like_mut();
            self.table_type.insert(conn_like, key, data)
        } else {
            Err(redis::RedisError::from((
                redis::ErrorKind::Io,
                "Redis connection not initialized",
            )))
        }
    }

    pub fn update_data_to_key(
        &mut self,
        key: &str,
        old_data: &[String],
        new_data: &[String],
    ) -> Result<(), redis::RedisError> {
        if let Some(conn) = self.redis_connection.as_mut() {
            let conn_like = conn.as_connection_like_mut();
            self.table_type.update(conn_like, key, old_data, new_data)
        } else {
            Err(redis::RedisError::from((
                redis::ErrorKind::Io,
                "Redis connection not initialized",
            )))
        }
    }
}

pub fn is_multi_key_pattern(prefix: &str) -> bool {
    prefix.contains('*') || prefix.contains('?') || prefix.contains('[')
}
