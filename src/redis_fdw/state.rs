use std::collections::HashMap;
use pgrx::{pg_sys::MemoryContext, prelude::*};
use crate::redis_fdw::{
    tables::{
        RedisTableOperations, 
        RedisHashTable, 
        RedisListTable, 
        RedisSetTable, 
        RedisStringTable, 
        RedisZSetTable
    },
    pushdown::{PushdownAnalysis, WhereClausePushdown}
};


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
    
    pub fn load_data(&mut self, conn: &mut redis::Connection, key_prefix: &str) -> Result<(), redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.load_data(conn, key_prefix),
            RedisTableType::Hash(table) => table.load_data(conn, key_prefix),
            RedisTableType::List(table) => table.load_data(conn, key_prefix),
            RedisTableType::Set(table) => table.load_data(conn, key_prefix),
            RedisTableType::ZSet(table) => table.load_data(conn, key_prefix),
            RedisTableType::None => Ok(()),
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
    
    pub fn insert(&mut self, conn: &mut redis::Connection, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.insert(conn, key_prefix, data),
            RedisTableType::Hash(table) => table.insert(conn, key_prefix, data),
            RedisTableType::List(table) => table.insert(conn, key_prefix, data),
            RedisTableType::Set(table) => table.insert(conn, key_prefix, data),
            RedisTableType::ZSet(table) => table.insert(conn, key_prefix, data),
            RedisTableType::None => Ok(()),
        }
    }
    
    pub fn delete(&mut self, conn: &mut redis::Connection, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.delete(conn, key_prefix, data),
            RedisTableType::Hash(table) => table.delete(conn, key_prefix, data),
            RedisTableType::List(table) => table.delete(conn, key_prefix, data),
            RedisTableType::Set(table) => table.delete(conn, key_prefix, data),
            RedisTableType::ZSet(table) => table.delete(conn, key_prefix, data),
            RedisTableType::None => Ok(()),
        }
    }
    
    pub fn update(&mut self, conn: &mut redis::Connection, key_prefix: &str, old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.update(conn, key_prefix, old_data, new_data),
            RedisTableType::Hash(table) => table.update(conn, key_prefix, old_data, new_data),
            RedisTableType::List(table) => table.update(conn, key_prefix, old_data, new_data),
            RedisTableType::Set(table) => table.update(conn, key_prefix, old_data, new_data),
            RedisTableType::ZSet(table) => table.update(conn, key_prefix, old_data, new_data),
            RedisTableType::None => Ok(()),
        }
    }
}

/// Read FDW state
pub struct RedisFdwState {
    pub tmp_ctx: MemoryContext,
    pub redis_connection: Option<redis::Connection>,
    pub database : i64,
    pub host_port : String,
    pub table_type: RedisTableType,
    pub table_key_prefix: String,
    pub opts: HashMap<String, String>,
    pub row_count: u32,
    pub key_attno : i16,
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
            key_attno : 0,
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
        let addr_port = format!("redis://{}/{}" ,self.host_port, self.database);
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
        let table_type = self.opts
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
                    // Apply pushdown conditions
                    match WhereClausePushdown::apply_pushdown_to_redis(
                        &analysis.pushable_conditions,
                        &self.table_type,
                        conn,
                        &self.table_key_prefix,
                    ) {
                        Ok(filtered_data) => {
                            log!("Pushdown optimization applied, loaded {} filtered items", filtered_data.len());
                            self.filtered_data = Some(filtered_data);
                            return;
                        }
                        Err(e) => {
                            error!("Pushdown failed, falling back to full scan: {:?}", e);
                        }
                    }
                }
            }
            
            // Fall back to loading all data
            let _ = self.table_type.load_data(conn, &self.table_key_prefix);
        }
    }

    /// Set pushdown analysis from planner
    pub fn set_pushdown_analysis(&mut self, analysis: PushdownAnalysis) {
        log!("Setting pushdown analysis: can_optimize={}, conditions={:?}", 
             analysis.can_optimize, analysis.pushable_conditions);
        self.pushdown_analysis = Some(analysis);
    }
    
    pub fn is_read_end(&self) -> bool {
        self.row_count >= self.table_type.data_len() as u32
    }

    pub fn data_len(&self) -> usize {
        // If we have filtered data from pushdown, use that
        if let Some(ref filtered_data) = self.filtered_data {
            return match &self.table_type {
                RedisTableType::Hash(_) => filtered_data.len() / 2, // key-value pairs
                RedisTableType::ZSet(_) => filtered_data.len() / 2, // member-score pairs
                _ => filtered_data.len(),
            };
        }
        
        // Otherwise use table type's data
        self.table_type.data_len()
    }

    /// Insert data using the appropriate table type
    pub fn insert_data(&mut self, data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(conn) = self.redis_connection.as_mut() {
            self.table_type.insert(conn, &self.table_key_prefix, data)
        } else {
            Err(redis::RedisError::from((redis::ErrorKind::IoError, "Redis connection not initialized")))
        }
    }

    /// Delete data using the appropriate table type
    pub fn delete_data(&mut self, data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(conn) = self.redis_connection.as_mut() {
            self.table_type.delete(conn, &self.table_key_prefix, data)
        } else {
            Err(redis::RedisError::from((redis::ErrorKind::IoError, "Redis connection not initialized")))
        }
    }

    /// Update data using the appropriate table type
    pub fn update_data(&mut self, old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(conn) = self.redis_connection.as_mut() {
            self.table_type.update(conn, &self.table_key_prefix, old_data, new_data)
        } else {
            Err(redis::RedisError::from((redis::ErrorKind::IoError, "Redis connection not initialized")))
        }
    }

    /// Get a row at the specified index
    pub fn get_row(&self, index: usize) -> Option<Vec<String>> {
        // If we have filtered data from pushdown, use that
        if let Some(ref filtered_data) = self.filtered_data {
            return self.get_row_from_filtered_data(filtered_data, index);
        }
        
        // Otherwise use table type's data
        self.table_type.get_row(index)
    }

    /// Get a row from filtered data based on table type
    fn get_row_from_filtered_data(&self, filtered_data: &[String], index: usize) -> Option<Vec<String>> {
        match &self.table_type {
            RedisTableType::Hash(_) => {
                // Hash data is stored as [key1, value1, key2, value2, ...]
                let data_index = index * 2;
                if data_index + 1 < filtered_data.len() {
                    Some(vec![
                        filtered_data[data_index].clone(),
                        filtered_data[data_index + 1].clone(),
                    ])
                } else {
                    None
                }
            }
            RedisTableType::List(_) => {
                // List data is stored as [element1, element2, ...]
                if index < filtered_data.len() {
                    Some(vec![filtered_data[index].clone()])
                } else {
                    None
                }
            }
            RedisTableType::Set(_) => {
                // Set data is stored as [member1, member2, ...]
                if index < filtered_data.len() {
                    Some(vec![filtered_data[index].clone()])
                } else {
                    None
                }
            }
            RedisTableType::ZSet(_) => {
                // ZSet data is stored as [member1, score1, member2, score2, ...]
                let data_index = index * 2;
                if data_index + 1 < filtered_data.len() {
                    Some(vec![
                        filtered_data[data_index].clone(),
                        filtered_data[data_index + 1].clone(),
                    ])
                } else {
                    None
                }
            }
            RedisTableType::String(_) => {
                // String data is stored as [value]
                if index == 0 && !filtered_data.is_empty() {
                    Some(vec![filtered_data[0].clone()])
                } else {
                    None
                }
            }
            RedisTableType::None => None,
        }
    }
}


