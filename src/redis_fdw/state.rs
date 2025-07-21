use crate::redis_fdw::{
    pushdown::PushdownAnalysis,
    tables::{
        RedisHashTable, RedisListTable, RedisSetTable, RedisStringTable, RedisTableOperations,
        RedisZSetTable,
    },
};
use pgrx::{pg_sys::MemoryContext, prelude::*};
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
        conn: &mut redis::Connection,
        key_prefix: &str,
        conditions: Option<&[crate::redis_fdw::pushdown::PushableCondition]>,
    ) -> Result<Option<Vec<String>>, redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.load_data(conn, key_prefix, conditions),
            RedisTableType::Hash(table) => table.load_data(conn, key_prefix, conditions),
            RedisTableType::List(table) => table.load_data(conn, key_prefix, conditions),
            RedisTableType::Set(table) => table.load_data(conn, key_prefix, conditions),
            RedisTableType::ZSet(table) => table.load_data(conn, key_prefix, conditions),
            RedisTableType::None => Ok(None),
        }
    }

    pub fn data_len(&self, filtered_data: Option<&[String]>) -> usize {
        match self {
            RedisTableType::String(table) => table.data_len(filtered_data),
            RedisTableType::Hash(table) => table.data_len(filtered_data),
            RedisTableType::List(table) => table.data_len(filtered_data),
            RedisTableType::Set(table) => table.data_len(filtered_data),
            RedisTableType::ZSet(table) => table.data_len(filtered_data),
            RedisTableType::None => 0,
        }
    }

    pub fn get_row(&self, index: usize, filtered_data: Option<&[String]>) -> Option<Vec<String>> {
        match self {
            RedisTableType::String(table) => table.get_row(index, filtered_data),
            RedisTableType::Hash(table) => table.get_row(index, filtered_data),
            RedisTableType::List(table) => table.get_row(index, filtered_data),
            RedisTableType::Set(table) => table.get_row(index, filtered_data),
            RedisTableType::ZSet(table) => table.get_row(index, filtered_data),
            RedisTableType::None => None,
        }
    }

    pub fn insert(
        &mut self,
        conn: &mut redis::Connection,
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
        conn: &mut redis::Connection,
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
        conn: &mut redis::Connection,
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
        operator: &crate::redis_fdw::pushdown::ComparisonOperator,
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
    pub redis_connection: Option<redis::Connection>,
    pub database: i64,
    pub host_port: String,
    pub table_type: RedisTableType,
    pub table_key_prefix: String,
    pub opts: HashMap<String, String>,
    pub row_count: u32,
    pub key_attno: i16,
    pub pushdown_analysis: Option<PushdownAnalysis>,
    pub filtered_data: Option<Vec<String>>,
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
            filtered_data: None,
        }
    }
}

impl RedisFdwState {
    /// Check if redis connection is initialized
    /// # Panics
    /// Panics if redis_connection is None
    /// # Returns
    /// A reference to the redis connection
    pub fn init_redis_connection_from_options(&mut self) {
        let addr_port = format!("redis://{}/{}", self.host_port, self.database);
        let client = redis::Client::open(addr_port).expect("Failed to create Redis client");
        self.redis_connection = Some(client.get_connection().expect("Failed to connect to Redis"));
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
                .unwrap_or_else(|_| panic!("Invalid `database` value: {}", db_str));
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
        self.load_data_with_pushdown();
    }

    /// Load data from Redis, applying pushdown optimizations if available
    fn load_data_with_pushdown(&mut self) {
        if let Some(conn) = self.redis_connection.as_mut() {
            if let Some(ref analysis) = self.pushdown_analysis {
                if analysis.can_optimize {
                    // Apply pushdown conditions using the table type's unified method
                    match self.table_type.load_data(
                        conn,
                        &self.table_key_prefix,
                        Some(&analysis.pushable_conditions),
                    ) {
                        Ok(Some(filtered_data)) => {
                            log!(
                                "Pushdown optimization applied, loaded {} filtered items",
                                filtered_data.len()
                            );
                            self.filtered_data = Some(filtered_data);
                            return;
                        }
                        Ok(None) => {
                            log!("Data loaded into table internal storage");
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
                .load_data(conn, &self.table_key_prefix, None);
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
        // If we have filtered data from pushdown, use that
        if let Some(ref filtered_data) = self.filtered_data {
            return self.table_type.data_len(Some(filtered_data));
        }

        // Otherwise use table type's data without filtered data
        self.table_type.data_len(None)
    }

    /// Insert data using the appropriate table type
    pub fn insert_data(&mut self, data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(conn) = self.redis_connection.as_mut() {
            self.table_type.insert(conn, &self.table_key_prefix, data)
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
            self.table_type.delete(conn, &self.table_key_prefix, data)
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
            self.table_type
                .update(conn, &self.table_key_prefix, old_data, new_data)
        } else {
            Err(redis::RedisError::from((
                redis::ErrorKind::IoError,
                "Redis connection not initialized",
            )))
        }
    }

    /// Get a row at the specified index
    pub fn get_row(&self, index: usize) -> Option<Vec<String>> {
        // If we have filtered data from pushdown, use that
        if let Some(ref filtered_data) = self.filtered_data {
            return self.table_type.get_row(index, Some(filtered_data));
        }

        // Otherwise use table type's data without filtered data
        self.table_type.get_row(index, None)
    }
}
