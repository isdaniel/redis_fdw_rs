use std::collections::HashMap;
use pgrx::pg_sys::MemoryContext;
use redis::aio::ConnectionManager;
use crate::redis_fdw::{
    tables::{
        RedisTableOperations, 
        RedisHashTable, 
        RedisListTable, 
        RedisSetTable, 
        RedisStringTable, 
        RedisZSetTable
    }
};
use crate::async_runtime::block_on_borrowed;


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
    
    pub fn load_data(&mut self, conn: &mut ConnectionManager, key_prefix: &str) -> Result<(), redis::RedisError> {
        block_on_borrowed(self.load_data_async(conn, key_prefix))
    }
    
    pub async fn load_data_async(&mut self, conn: &mut ConnectionManager, key_prefix: &str) -> Result<(), redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.load_data(conn, key_prefix).await,
            RedisTableType::Hash(table) => table.load_data(conn, key_prefix).await,
            RedisTableType::List(table) => table.load_data(conn, key_prefix).await,
            RedisTableType::Set(table) => table.load_data(conn, key_prefix).await,
            RedisTableType::ZSet(table) => table.load_data(conn, key_prefix).await,
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
    
    pub fn insert(&mut self, conn: &mut ConnectionManager, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        block_on_borrowed(self.insert_async(conn, key_prefix, data))
    }
    
    pub async fn insert_async(&mut self, conn: &mut ConnectionManager, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.insert(conn, key_prefix, data).await,
            RedisTableType::Hash(table) => table.insert(conn, key_prefix, data).await,
            RedisTableType::List(table) => table.insert(conn, key_prefix, data).await,
            RedisTableType::Set(table) => table.insert(conn, key_prefix, data).await,
            RedisTableType::ZSet(table) => table.insert(conn, key_prefix, data).await,
            RedisTableType::None => Ok(()),
        }
    }
    
    pub fn delete(&mut self, conn: &mut ConnectionManager, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        block_on_borrowed(self.delete_async(conn, key_prefix, data))
    }
    
    pub async fn delete_async(&mut self, conn: &mut ConnectionManager, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.delete(conn, key_prefix, data).await,
            RedisTableType::Hash(table) => table.delete(conn, key_prefix, data).await,
            RedisTableType::List(table) => table.delete(conn, key_prefix, data).await,
            RedisTableType::Set(table) => table.delete(conn, key_prefix, data).await,
            RedisTableType::ZSet(table) => table.delete(conn, key_prefix, data).await,
            RedisTableType::None => Ok(()),
        }
    }
    
    pub fn update(&mut self, conn: &mut ConnectionManager, key_prefix: &str, old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        block_on_borrowed(self.update_async(conn, key_prefix, old_data, new_data))
    }
    
    pub async fn update_async(&mut self, conn: &mut ConnectionManager, key_prefix: &str, old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.update(conn, key_prefix, old_data, new_data).await,
            RedisTableType::Hash(table) => table.update(conn, key_prefix, old_data, new_data).await,
            RedisTableType::List(table) => table.update(conn, key_prefix, old_data, new_data).await,
            RedisTableType::Set(table) => table.update(conn, key_prefix, old_data, new_data).await,
            RedisTableType::ZSet(table) => table.update(conn, key_prefix, old_data, new_data).await,
            RedisTableType::None => Ok(()),
        }
    }
}

/// Read FDW state (async version)
pub struct RedisFdwState {
    pub tmp_ctx: MemoryContext,
    pub redis_connection: Option<ConnectionManager>,
    pub database: i64,
    pub host_port: String,
    pub table_type: RedisTableType,
    pub table_key_prefix: String,
    pub opts: HashMap<String, String>,
    pub row_count: u32,
    pub key_attno: i16,
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
            key_attno : 0
        }
    }
}

impl RedisFdwState {

    /// Initialize async Redis connection from options
    pub fn init_redis_connection_from_options(&mut self) {
        let addr_port = format!("redis://{}/{}", self.host_port, self.database);
        
        let connection_manager = block_on_borrowed(async {
            let client = redis::Client::open(addr_port)
                .expect("Failed to create Redis client");
            redis::aio::ConnectionManager::new(client)
                .await
                .expect("Failed to create Redis connection manager")
        });
        
        self.redis_connection = Some(connection_manager);
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
        
        // Load data from Redis
        if let Some(conn) = self.redis_connection.as_mut() {
            let _ = self.table_type.load_data(conn, &self.table_key_prefix);
        }
    }
    
    pub fn is_read_end(&self) -> bool {
        self.row_count >= self.table_type.data_len() as u32
    }

    pub fn data_len(&self) -> usize {
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
        self.table_type.get_row(index)
    }
}


