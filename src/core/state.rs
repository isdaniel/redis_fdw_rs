use crate::{
    core::connection::RedisConnectionType,
    query::pushdown_types::{PushableCondition, PushdownAnalysis},
    tables::types::{LoadDataResult, RedisTableType},
};
use pgrx::{pg_sys::MemoryContext, prelude::*};
use redis::cluster::ClusterClient;
use std::collections::HashMap;

/// Read FDW state
pub struct RedisFdwState {
    pub tmp_ctx: MemoryContext,
    pub redis_connection: Option<RedisConnectionType>,
    pub database: i64,
    pub host_port: String,
    pub table_type: RedisTableType,
    pub table_key_prefix: String,
    pub opts: HashMap<String, String>,
    pub row_count: u32,
    pub key_attno: i16,
    pub pushdown_analysis: Option<PushdownAnalysis>,
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
        }
    }
}

impl RedisFdwState {
    /// Check if redis connection is initialized and create appropriate connection type
    /// Supports both single-node and cluster connections
    /// # Panics
    /// Panics if connection fails
    pub fn init_redis_connection_from_options(&mut self) {
        // Check if host_port contains multiple nodes (cluster mode)
        if self.host_port.contains(',') {
            // Cluster mode: parse multiple node addresses
            let nodes: Vec<String> = self
                .host_port
                .split(',')
                .map(|node| {
                    let trimmed = node.trim();
                    // Add redis:// prefix if not present and format with database
                    if trimmed.starts_with("redis://") {
                        format!("{}/{}", trimmed, self.database)
                    } else {
                        format!("redis://{}/{}", trimmed, self.database)
                    }
                })
                .collect();

            log!("Connecting to Redis cluster with nodes: {:?}", nodes);
            let cluster_client =
                ClusterClient::new(nodes).expect("Failed to create Redis cluster client");
            let cluster_connection = cluster_client
                .get_connection()
                .expect("Failed to connect to Redis cluster");
            self.redis_connection = Some(RedisConnectionType::Cluster(cluster_connection));
        } else {
            // Single node mode
            let addr_port = format!("redis://{}/{}", self.host_port, self.database);
            log!("Connecting to single Redis node: {}", addr_port);
            let client = redis::Client::open(addr_port).expect("Failed to create Redis client");
            let connection = client.get_connection().expect("Failed to connect to Redis");
            self.redis_connection = Some(RedisConnectionType::Single(connection));
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
    }

    pub fn set_table_type(&mut self) {
        let table_type = self
            .opts
            .get("table_type")
            .expect("`table_type` option is required for redis_fdw");

        self.table_type = RedisTableType::from_str(table_type);

        // Load data from Redis (will be optimized if pushdown conditions exist)
        self.load_data();
    }

    /// Load data from Redis, applying pushdown optimizations if available
    fn load_data(&mut self) {
        let pushable_conditions: Option<&[PushableCondition]> = None;
        if let Some(conn) = self.redis_connection.as_mut() {
            let conn_like = conn.as_connection_like_mut();
            if let Some(ref analysis) = self.pushdown_analysis {
                if analysis.can_optimize {
                    // Apply pushdown conditions using the table type's unified method
                    match self.table_type.load_data(
                        conn_like,
                        &self.table_key_prefix,
                        Some(&analysis.pushable_conditions),
                    ) {
                        Ok(LoadDataResult::PushdownApplied(filtered_data)) => {
                            log!(
                                "Pushdown optimization applied, loaded {} filtered items",
                                filtered_data.len()
                            );
                            return;
                        }
                        Ok(LoadDataResult::LoadedToInternal) => {
                            log!("Data loaded into table internal storage");
                            return;
                        }
                        Ok(LoadDataResult::Empty) => {
                            log!("No data found for pushdown conditions");
                            return;
                        }
                        Err(e) => {
                            error!("Pushdown failed, falling back to full scan: {:?}", e);
                        }
                    }
                }
            }

            // Fall back to loading all data without pushdown
            let _ = self
                .table_type
                .load_data(conn_like, &self.table_key_prefix, None);
        }
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

    pub fn is_read_end(&self) -> bool {
        self.row_count >= self.data_len() as u32
    }

    pub fn data_len(&self) -> usize {
        self.table_type.data_len()
    }

    /// Insert data using the appropriate table type
    pub fn insert_data(&mut self, data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(conn) = self.redis_connection.as_mut() {
            let conn_like = conn.as_connection_like_mut();
            self.table_type
                .insert(conn_like, &self.table_key_prefix, data)
        } else {
            Err(redis::RedisError::from((
                redis::ErrorKind::IoError,
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
                redis::ErrorKind::IoError,
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
                redis::ErrorKind::IoError,
                "Redis connection not initialized",
            )))
        }
    }

    /// Get a row at the specified index
    pub fn get_row(&self, index: usize) -> Option<Vec<String>> {
        self.table_type.get_row(index)
    }
}
