use crate::redis_fdw::{
    data_set::LoadDataResult, pushdown::{self, ComparisonOperator, PushableCondition, PushdownAnalysis}, tables::{
        interface::RedisConnectionType, RedisHashTable, RedisListTable, RedisSetTable, RedisStringTable, RedisTableOperations, RedisZSetTable
    }
};
use pgrx::{pg_sys::MemoryContext, prelude::*};
use redis::cluster::ClusterClient;
use std::collections::HashMap;

/// Enum representing different Redis table types with their implementations
#[derive(Debug, Clone)]
pub enum RedisTableType {
    String(RedisStringTable),
    Hash(RedisHashTable),
    List(RedisListTable),
    Set(RedisSetTable),
    ZSet(RedisZSetTable),
    None,
}

impl RedisTableType {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "string" => RedisTableType::String(RedisStringTable::new()),
            "hash" => RedisTableType::Hash(RedisHashTable::new()),
            "list" => RedisTableType::List(RedisListTable::new()),
            "set" => RedisTableType::Set(RedisSetTable::new()),
            "zset" => RedisTableType::ZSet(RedisZSetTable::new()),
            _ => RedisTableType::None,
        }
    }

    pub fn load_data(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        conditions: Option<&[PushableCondition]>,
    ) -> Result<LoadDataResult, redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.load_data(conn, key_prefix, conditions),
            RedisTableType::Hash(table) => table.load_data(conn, key_prefix, conditions),
            RedisTableType::List(table) => table.load_data(conn, key_prefix, conditions),
            RedisTableType::Set(table) => table.load_data(conn, key_prefix, conditions),
            RedisTableType::ZSet(table) => table.load_data(conn, key_prefix, conditions),
            RedisTableType::None => Ok(LoadDataResult::Empty),
        }
    }

    pub fn data_len(&self) -> usize {
        match self {
            RedisTableType::String(table) => table.data_len(),
            RedisTableType::Hash(table) => table.data_len(),
            RedisTableType::List(table) => table.data_len(),
            RedisTableType::Set(table) => table.data_len(),
            RedisTableType::ZSet(table) => table.data_len(),
            RedisTableType::None => 0,
        }
    }

    pub fn get_row(&self, index: usize) -> Option<Vec<String>> {
        match self {
            RedisTableType::String(table) => table.get_row(index),
            RedisTableType::Hash(table) => table.get_row(index),
            RedisTableType::List(table) => table.get_row(index),
            RedisTableType::Set(table) => table.get_row(index),
            RedisTableType::ZSet(table) => table.get_row(index),
            RedisTableType::None => None,
        }
    }

    pub fn insert(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.insert(conn, key_prefix, data),
            RedisTableType::Hash(table) => table.insert(conn, key_prefix, data),
            RedisTableType::List(table) => table.insert(conn, key_prefix, data),
            RedisTableType::Set(table) => table.insert(conn, key_prefix, data),
            RedisTableType::ZSet(table) => table.insert(conn, key_prefix, data),
            RedisTableType::None => Ok(()),
        }
    }

    pub fn delete(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        data: &[String],
    ) -> Result<(), redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.delete(conn, key_prefix, data),
            RedisTableType::Hash(table) => table.delete(conn, key_prefix, data),
            RedisTableType::List(table) => table.delete(conn, key_prefix, data),
            RedisTableType::Set(table) => table.delete(conn, key_prefix, data),
            RedisTableType::ZSet(table) => table.delete(conn, key_prefix, data),
            RedisTableType::None => Ok(()),
        }
    }

    pub fn update(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        old_data: &[String],
        new_data: &[String],
    ) -> Result<(), redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.update(conn, key_prefix, old_data, new_data),
            RedisTableType::Hash(table) => table.update(conn, key_prefix, old_data, new_data),
            RedisTableType::List(table) => table.update(conn, key_prefix, old_data, new_data),
            RedisTableType::Set(table) => table.update(conn, key_prefix, old_data, new_data),
            RedisTableType::ZSet(table) => table.update(conn, key_prefix, old_data, new_data),
            RedisTableType::None => Ok(()),
        }
    }

    /// Check if this table type supports a specific pushdown operator
    pub fn supports_pushdown(
        &self,
        operator: &ComparisonOperator,
    ) -> bool {
        match self {
            RedisTableType::String(table) => table.supports_pushdown(operator),
            RedisTableType::Hash(table) => table.supports_pushdown(operator),
            RedisTableType::List(table) => table.supports_pushdown(operator),
            RedisTableType::Set(table) => table.supports_pushdown(operator),
            RedisTableType::ZSet(table) => table.supports_pushdown(operator),
            RedisTableType::None => false,
        }
    }
}

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
            let nodes: Vec<String> = self.host_port
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
            let cluster_client = ClusterClient::new(nodes).expect("Failed to create Redis cluster client");
            let cluster_connection = cluster_client.get_connection().expect("Failed to connect to Redis cluster");
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
            self.table_type.insert(conn_like, &self.table_key_prefix, data)
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
            self.table_type.delete(conn_like, &self.table_key_prefix, data)
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
