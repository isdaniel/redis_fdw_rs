use crate::{
    core::{
        connection::RedisConnectionType,
        connection_factory::{RedisConnectionConfig, RedisConnectionFactory},
    },
    query::pushdown_types::PushdownAnalysis,
    tables::types::{LoadDataResult, RedisTableType},
};
use pgrx::{pg_sys::MemoryContext, prelude::*};
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
