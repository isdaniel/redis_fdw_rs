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
    tables::types::{DataContainer, DataSet, RedisTableType},
};
use pgrx::{pg_sys, pg_sys::MemoryContext, prelude::*};
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
    /// Whether to error (true) or warn (false) on multi-key prefix mismatch
    pub strict_key_prefix: bool,
    /// Cached TTL value for single-key mode (avoids repeated TTL calls per row)
    pub cached_ttl: Option<i64>,
    /// Cached TTL values for multi-key mode (batch-fetched via pipeline)
    pub multi_key_ttl_cache: HashMap<String, i64>,
    /// Whether this is a join pushdown scan (FDW-to-FDW on same server)
    pub is_join_scan: bool,
    /// Join execution state (populated during begin_foreign_scan for join scans)
    pub join_state: Option<crate::join::types::RedisJoinState>,
    /// Whether the join has been executed (lazy: execute on first iterate call)
    pub join_executed: bool,
    /// Column names from the foreign table's tuple descriptor
    pub column_names: Vec<String>,
    /// Whether this is a parameterized scan (receives join key from outer NestLoop)
    pub is_parameterized: bool,
    /// Column index (0-based, after TTL strip) that receives the parameter value
    pub param_column: usize,
    /// Type OID of the parameter expression
    pub param_type_oid: pg_sys::Oid,
    /// ExprState for evaluating the parameterized expression at runtime
    pub param_expr_state: *mut pg_sys::ExprState,
    /// PlanState pointer for expression evaluation context
    pub param_plan_state: *mut pg_sys::PlanState,
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
            batch_size: 5000,
            ttl_column_index: None,
            default_ttl: None,
            is_multi_key: false,
            strict_key_prefix: false,
            cached_ttl: None,
            multi_key_ttl_cache: HashMap::new(),
            is_join_scan: false,
            join_state: None,
            join_executed: false,
            column_names: Vec::new(),
            is_parameterized: false,
            param_column: 0,
            param_type_oid: pg_sys::InvalidOid,
            param_expr_state: std::ptr::null_mut(),
            param_plan_state: std::ptr::null_mut(),
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

        if let Some(skp) = self.opts.get("strict_key_prefix") {
            self.strict_key_prefix = skp == "true";
        }
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

            // Use direct load for ZSet score-range conditions (ZRANGEBYSCORE is O(log N + M))
            if let RedisTableType::ZSet(ref z) = self.table_type {
                let score_idx = z.score_column_index;
                let has_score_range = analysis.pushable_conditions.iter().any(|c| {
                    c.column_index == score_idx
                        && matches!(
                            c.operator,
                            ComparisonOperator::GreaterThan
                                | ComparisonOperator::GreaterThanOrEqual
                                | ComparisonOperator::LessThan
                                | ComparisonOperator::LessThanOrEqual
                        )
                });
                if has_score_range {
                    return true;
                }
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
        self.multi_key_ttl_cache.clear();
        loop {
            pgrx::check_for_interrupts!();

            let mut cmd = redis::cmd("SCAN");
            cmd.arg(self.scan_cursor)
                .arg("MATCH")
                .arg(&self.table_key_prefix)
                .arg("COUNT")
                .arg(self.batch_size);

            let scan_type = self.table_type.redis_type_name();
            if !scan_type.is_empty() {
                cmd.arg("TYPE").arg(scan_type);
            }

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

            // Batch-fetch TTLs for the scanned keys if TTL column is present
            if self.ttl_column_index.is_some() {
                let mut pipe = redis::pipe();
                for key in &keys {
                    pipe.cmd("TTL").arg(key);
                }
                let ttls: Vec<i64> = match pipe.query(conn) {
                    Ok(v) => v,
                    Err(e) => {
                        log!("WARNING: Redis pipeline TTL error: {}", e);
                        vec![-2; keys.len()]
                    }
                };
                for (key, ttl) in keys.iter().zip(ttls) {
                    self.multi_key_ttl_cache.insert(key.clone(), ttl);
                }
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
    /// Uses Redis pipelining for batch operations to minimize network round-trips.
    fn load_multi_key_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        keys: &[String],
    ) -> usize {
        let all_rows = match self.table_type.load_multi_key_data(conn, keys) {
            Ok(rows) => rows,
            Err(e) => {
                pgrx::error!("Redis multi-key load error: {}", e);
            }
        };

        const MULTI_KEY_WARN_THRESHOLD: usize = 1_000_000;
        if all_rows.len() > MULTI_KEY_WARN_THRESHOLD {
            pgrx::warning!(
                "Redis FDW: multi-key batch accumulated {} elements, query may use excessive memory",
                all_rows.len()
            );
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
        self.table_type.multi_key_columns_per_row()
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
    /// In multi-key mode, uses pre-fetched cache from `batch_read_ttls`.
    pub fn read_ttl(&mut self, key: &str) -> i64 {
        if !self.is_multi_key {
            if let Some(cached) = self.cached_ttl {
                return cached;
            }
        } else if let Some(&cached) = self.multi_key_ttl_cache.get(key) {
            return cached;
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

    /// Batch insert multiple rows using Redis pipelining.
    /// Handles cluster vs standalone internally. Applies TTL per row.
    pub fn batch_insert_data(&mut self, rows: &[(Vec<String>, Option<i64>)]) -> Result<(), String> {
        if let Some(ref mut conn) = self.redis_connection {
            if let Some(cluster_conn) = conn.as_cluster_connection_mut() {
                Self::batch_insert_cluster(
                    cluster_conn,
                    &self.table_type,
                    &self.table_key_prefix,
                    self.is_multi_key,
                    self.strict_key_prefix,
                    self.default_ttl,
                    rows,
                )
            } else {
                let conn_like = conn.as_connection_like_mut();
                Self::batch_insert_standalone(
                    conn_like,
                    &self.table_type,
                    &self.table_key_prefix,
                    self.is_multi_key,
                    self.strict_key_prefix,
                    self.default_ttl,
                    rows,
                )
            }
        } else {
            Err("Redis connection not available for batch insert".to_string())
        }
    }

    fn batch_insert_standalone(
        conn: &mut dyn redis::ConnectionLike,
        table_type: &RedisTableType,
        table_key_prefix: &str,
        is_multi_key: bool,
        strict_key_prefix: bool,
        default_ttl: Option<i64>,
        rows: &[(Vec<String>, Option<i64>)],
    ) -> Result<(), String> {
        let mut pipe = redis::pipe();
        let mut has_cmds = false;
        let static_prefix = if is_multi_key {
            extract_static_prefix(table_key_prefix)
        } else {
            ""
        };

        for (data, row_ttl) in rows {
            let (key, row_data) = if is_multi_key {
                if data.is_empty() {
                    continue;
                }
                validate_key_prefix(
                    data[0].as_str(),
                    static_prefix,
                    table_key_prefix,
                    strict_key_prefix,
                );
                (data[0].as_str(), &data[1..])
            } else {
                (table_key_prefix, data.as_slice())
            };
            if Self::add_insert_to_pipeline(&mut pipe, table_type, key, row_data) {
                has_cmds = true;
                has_cmds |= Self::add_ttl_to_pipeline(&mut pipe, key, *row_ttl, default_ttl);
            }
        }

        if has_cmds {
            pipe.query::<()>(conn)
                .map_err(|e| format!("Redis batch insert pipeline failed: {}", e))?;
        }

        Ok(())
    }

    fn batch_insert_cluster(
        cluster_conn: &mut redis::cluster::ClusterConnection,
        table_type: &RedisTableType,
        table_key_prefix: &str,
        is_multi_key: bool,
        strict_key_prefix: bool,
        default_ttl: Option<i64>,
        rows: &[(Vec<String>, Option<i64>)],
    ) -> Result<(), String> {
        let mut pipe = redis::cluster::cluster_pipe();
        let mut has_cmds = false;
        let static_prefix = if is_multi_key {
            extract_static_prefix(table_key_prefix)
        } else {
            ""
        };

        for (data, row_ttl) in rows {
            let (key, row_data) = if is_multi_key {
                if data.is_empty() {
                    continue;
                }
                validate_key_prefix(
                    data[0].as_str(),
                    static_prefix,
                    table_key_prefix,
                    strict_key_prefix,
                );
                (data[0].as_str(), &data[1..])
            } else {
                (table_key_prefix, data.as_slice())
            };
            if Self::add_insert_to_cluster_pipeline(&mut pipe, table_type, key, row_data) {
                has_cmds = true;
                has_cmds |=
                    Self::add_ttl_to_cluster_pipeline(&mut pipe, key, *row_ttl, default_ttl);
            }
        }

        if has_cmds {
            pipe.query::<()>(cluster_conn)
                .map_err(|e| format!("Redis cluster batch insert pipeline failed: {}", e))?;
        }

        Ok(())
    }

    fn add_insert_to_pipeline(
        pipe: &mut redis::Pipeline,
        table_type: &RedisTableType,
        key: &str,
        data: &[String],
    ) -> bool {
        match table_type {
            RedisTableType::Hash(_) => {
                if data.len() >= 2 {
                    pipe.cmd("HSET").arg(key).arg(&data[0]).arg(&data[1]);
                    return true;
                }
            }
            RedisTableType::List(_) => {
                if !data.is_empty() {
                    pipe.cmd("RPUSH").arg(key).arg(&data[0]);
                    return true;
                }
            }
            RedisTableType::Set(_) => {
                if !data.is_empty() {
                    pipe.cmd("SADD").arg(key).arg(&data[0]);
                    return true;
                }
            }
            RedisTableType::ZSet(_) => {
                if data.len() >= 2 {
                    if data[1].parse::<f64>().is_ok() {
                        pipe.cmd("ZADD").arg(key).arg(&data[1]).arg(&data[0]);
                        return true;
                    } else {
                        pgrx::warning!(
                            "ZSet batch insert: invalid score '{}' for member '{}', row skipped",
                            data[1],
                            data[0]
                        );
                    }
                }
            }
            RedisTableType::String(_) => {
                if !data.is_empty() {
                    pipe.cmd("SET").arg(key).arg(&data[0]);
                    return true;
                }
            }
            RedisTableType::Stream(_) => {
                // data format after transform: [id, field1, val1, field2, val2, ...]
                if data.len() >= 3 {
                    let id = if data[0] == "*" || data[0].contains('-') {
                        data[0].as_str()
                    } else {
                        "*"
                    };
                    let mut cmd = redis::cmd("XADD");
                    cmd.arg(key).arg(id);
                    for chunk in data[1..].chunks(2) {
                        if chunk.len() == 2 {
                            cmd.arg(&chunk[0]).arg(&chunk[1]);
                        }
                    }
                    pipe.add_command(cmd);
                    return true;
                }
            }
            RedisTableType::None => {}
        }
        false
    }

    fn add_insert_to_cluster_pipeline(
        pipe: &mut redis::cluster::ClusterPipeline,
        table_type: &RedisTableType,
        key: &str,
        data: &[String],
    ) -> bool {
        match table_type {
            RedisTableType::Hash(_) => {
                if data.len() >= 2 {
                    pipe.cmd("HSET").arg(key).arg(&data[0]).arg(&data[1]);
                    return true;
                }
            }
            RedisTableType::List(_) => {
                if !data.is_empty() {
                    pipe.cmd("RPUSH").arg(key).arg(&data[0]);
                    return true;
                }
            }
            RedisTableType::Set(_) => {
                if !data.is_empty() {
                    pipe.cmd("SADD").arg(key).arg(&data[0]);
                    return true;
                }
            }
            RedisTableType::ZSet(_) => {
                if data.len() >= 2 {
                    if data[1].parse::<f64>().is_ok() {
                        pipe.cmd("ZADD").arg(key).arg(&data[1]).arg(&data[0]);
                        return true;
                    } else {
                        pgrx::warning!(
                            "ZSet batch insert: invalid score '{}' for member '{}', row skipped",
                            data[1],
                            data[0]
                        );
                    }
                }
            }
            RedisTableType::String(_) => {
                if !data.is_empty() {
                    pipe.cmd("SET").arg(key).arg(&data[0]);
                    return true;
                }
            }
            RedisTableType::Stream(_) => {
                // data format after transform: [id, field1, val1, field2, val2, ...]
                if data.len() >= 3 {
                    let id = if data[0] == "*" || data[0].contains('-') {
                        data[0].as_str()
                    } else {
                        "*"
                    };
                    let mut cmd = redis::cmd("XADD");
                    cmd.arg(key).arg(id);
                    for chunk in data[1..].chunks(2) {
                        if chunk.len() == 2 {
                            cmd.arg(&chunk[0]).arg(&chunk[1]);
                        }
                    }
                    pipe.add_command(cmd);
                    return true;
                }
            }
            RedisTableType::None => {}
        }
        false
    }

    fn add_ttl_to_pipeline(
        pipe: &mut redis::Pipeline,
        key: &str,
        row_ttl: Option<i64>,
        default_ttl: Option<i64>,
    ) -> bool {
        let effective_ttl = match row_ttl {
            Some(0) => return false,
            Some(t) => t,
            None => match default_ttl {
                Some(t) => t,
                None => return false,
            },
        };
        if effective_ttl > 0 {
            pipe.cmd("EXPIRE").arg(key).arg(effective_ttl);
            true
        } else if effective_ttl == -1 {
            pipe.cmd("PERSIST").arg(key);
            true
        } else {
            false
        }
    }

    fn add_ttl_to_cluster_pipeline(
        pipe: &mut redis::cluster::ClusterPipeline,
        key: &str,
        row_ttl: Option<i64>,
        default_ttl: Option<i64>,
    ) -> bool {
        let effective_ttl = match row_ttl {
            Some(0) => return false,
            Some(t) => t,
            None => match default_ttl {
                Some(t) => t,
                None => return false,
            },
        };
        if effective_ttl > 0 {
            pipe.cmd("EXPIRE").arg(key).arg(effective_ttl);
            true
        } else if effective_ttl == -1 {
            pipe.cmd("PERSIST").arg(key);
            true
        } else {
            false
        }
    }

    /// Delete a Redis key directly (for multi-key mode DELETE).
    pub fn delete_key(&mut self, key: &str) -> Result<(), redis::RedisError> {
        if let Some(conn) = self.redis_connection.as_mut() {
            let conn_like = conn.as_connection_like_mut();
            redis::cmd("DEL").arg(key).query::<()>(conn_like)
        } else {
            Err(redis::RedisError::from((
                redis::ErrorKind::Io,
                "Redis connection not initialized",
            )))
        }
    }

    /// Execute a parameterized point-lookup for a single value.
    /// Used during NestLoop joins to fetch only the matching row instead of full scan.
    pub fn parameterized_lookup(&mut self, param_value: &str) -> bool {
        let conn = match self.redis_connection.as_mut() {
            Some(c) => c.as_connection_like_mut(),
            None => return false,
        };

        match &mut self.table_type {
            RedisTableType::Hash(ref mut t) => {
                if self.param_column == t.pushdown_column_index {
                    let val: Option<String> = match redis::cmd("HGET")
                        .arg(&self.table_key_prefix)
                        .arg(param_value)
                        .query(conn)
                    {
                        Ok(v) => v,
                        Err(e) => {
                            pgrx::warning!(
                                "redis_fdw: HGET failed during parameterized lookup: {}",
                                e
                            );
                            None
                        }
                    };
                    if let Some(v) = val {
                        t.dataset = DataSet::Filtered(vec![param_value.to_string(), v]);
                        return true;
                    }
                    t.dataset = DataSet::Empty;
                }
                false
            }
            RedisTableType::Set(ref mut t) => {
                let exists: bool = match redis::cmd("SISMEMBER")
                    .arg(&self.table_key_prefix)
                    .arg(param_value)
                    .query(conn)
                {
                    Ok(v) => v,
                    Err(e) => {
                        pgrx::warning!(
                            "redis_fdw: SISMEMBER failed during parameterized lookup: {}",
                            e
                        );
                        false
                    }
                };
                if exists {
                    t.dataset = DataSet::Filtered(vec![param_value.to_string()]);
                    true
                } else {
                    t.dataset = DataSet::Empty;
                    false
                }
            }
            RedisTableType::ZSet(ref mut t) => {
                if self.param_column == t.pushdown_column_index {
                    let score: Option<f64> = match redis::cmd("ZSCORE")
                        .arg(&self.table_key_prefix)
                        .arg(param_value)
                        .query(conn)
                    {
                        Ok(v) => v,
                        Err(e) => {
                            pgrx::warning!(
                                "redis_fdw: ZSCORE failed during parameterized lookup: {}",
                                e
                            );
                            None
                        }
                    };
                    if let Some(s) = score {
                        t.dataset = DataSet::Complete(DataContainer::ZSet(vec![(
                            param_value.to_string(),
                            s,
                        )]));
                        return true;
                    }
                    t.dataset = DataSet::Empty;
                }
                false
            }
            RedisTableType::String(ref mut t) => {
                let expected_col = crate::core::column_utils::compute_pushdown_column_index(
                    self.ttl_column_index,
                    false,
                );
                if self.is_multi_key && self.param_column == expected_col {
                    let val: Option<String> = match redis::cmd("GET").arg(param_value).query(conn) {
                        Ok(v) => v,
                        Err(e) => {
                            pgrx::warning!(
                                "redis_fdw: GET failed during parameterized lookup: {}",
                                e
                            );
                            None
                        }
                    };
                    if let Some(v) = val {
                        t.dataset = DataSet::Filtered(vec![param_value.to_string(), v]);
                        return true;
                    }
                    t.dataset = DataSet::Empty;
                }
                false
            }
            _ => false,
        }
    }
}

pub fn is_multi_key_pattern(prefix: &str) -> bool {
    prefix.contains(['*', '?', '['])
}

/// Extract the static (non-glob) prefix from a multi-key pattern.
/// E.g. "user:*" → "user:", "session:?:data" → "session:", "key:[abc]" → "key:"
pub fn extract_static_prefix(pattern: &str) -> &str {
    let glob_pos = pattern.find(['*', '?', '[']).unwrap_or(pattern.len());
    &pattern[..glob_pos]
}

/// Validate that a key matches the table's key prefix pattern.
/// Accepts pre-calculated `static_prefix` to avoid redundant computation in batch loops.
/// Emits warning or error depending on `strict` flag.
pub fn validate_key_prefix(key: &str, static_prefix: &str, pattern: &str, strict: bool) {
    if !static_prefix.is_empty() && !key.starts_with(static_prefix) {
        let msg = format!(
            "redis_fdw: inserted key '{}' does not match table pattern '{}'. This key won't appear in SELECT results.",
            key, pattern
        );
        if strict {
            pgrx::error!("{}", msg);
        } else {
            pgrx::warning!("{}", msg);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_static_prefix() {
        assert_eq!(extract_static_prefix("user:*"), "user:");
        assert_eq!(extract_static_prefix("session:?:data"), "session:");
        assert_eq!(extract_static_prefix("key:[abc]"), "key:");
        assert_eq!(extract_static_prefix("*"), "");
        assert_eq!(extract_static_prefix("no_glob_here"), "no_glob_here");
        assert_eq!(extract_static_prefix("prefix:sub:*"), "prefix:sub:");
    }

    #[test]
    fn test_is_multi_key_pattern() {
        assert!(is_multi_key_pattern("prefix:*"));
        assert!(is_multi_key_pattern("user:?:name"));
        assert!(is_multi_key_pattern("key:[abc]"));
        assert!(!is_multi_key_pattern("simple:prefix:"));
        assert!(!is_multi_key_pattern("no_glob_here"));
    }
}
