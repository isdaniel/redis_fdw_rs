use std::collections::HashMap;
use std::sync::OnceLock;
use pgrx::{log, pg_sys::MemoryContext};
use redis::aio::ConnectionManager;
use tokio::{runtime::{Builder, Runtime}, time::{timeout, Duration}};
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

/// Global Redis runtime that's created only once
static GLOBAL_REDIS_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Initialize the global Redis runtime (call this once during extension initialization)
pub fn init_global_redis_runtime() {
    GLOBAL_REDIS_RUNTIME.get_or_init(|| {
        let worker_threads = num_cpus::get().min(4).max(16); // Between 4-16 threads
        log!("Initializing global Redis runtime with {} worker threads", worker_threads);
        
        Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name("redis-fdw-worker")
            .enable_all()
            .build()
            .expect("Failed to create global Redis runtime")
    });
}

#[inline]
pub fn get_global_redis_runtime() -> &'static Runtime {
    GLOBAL_REDIS_RUNTIME.get().expect("Global Redis runtime not initialized. Call init_global_redis_runtime() first.")
}


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
    
    pub async fn load_data(&mut self, conn: &mut ConnectionManager, key_prefix: &str) -> Result<(), redis::RedisError> {
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
    
    pub async fn insert(&mut self, conn: &mut ConnectionManager, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.insert(conn, key_prefix, data).await,
            RedisTableType::Hash(table) => table.insert(conn, key_prefix, data).await,
            RedisTableType::List(table) => table.insert(conn, key_prefix, data).await,
            RedisTableType::Set(table) => table.insert(conn, key_prefix, data).await,
            RedisTableType::ZSet(table) => table.insert(conn, key_prefix, data).await,
            RedisTableType::None => Ok(()),
        }
    }
    
    pub async fn delete(&mut self, conn: &mut ConnectionManager, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError> {
        match self {
            RedisTableType::String(table) => table.delete(conn, key_prefix, data).await,
            RedisTableType::Hash(table) => table.delete(conn, key_prefix, data).await,
            RedisTableType::List(table) => table.delete(conn, key_prefix, data).await,
            RedisTableType::Set(table) => table.delete(conn, key_prefix, data).await,
            RedisTableType::ZSet(table) => table.delete(conn, key_prefix, data).await,
            RedisTableType::None => Ok(()),
        }
    }
    
    pub async fn update(&mut self, conn: &mut ConnectionManager, key_prefix: &str, old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
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

/// Read FDW state
pub struct RedisFdwState {
    pub tmp_ctx: MemoryContext,
    pub redis_connection: Option<ConnectionManager>,
    pub database : i64,
    pub host_port : String,
    pub table_type: RedisTableType,
    pub table_key_prefix: String,
    pub opts: HashMap<String, String>,
    pub row_count: u32,
    pub key_attno : i16,
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
        log!("Creating Redis connection manager for {}", addr_port);
        let client = redis::Client::open(addr_port).expect("Failed to create Redis client");

        let runtime = get_global_redis_runtime();
        let conn_manager = runtime.block_on(async {
            match timeout(Duration::from_secs(5), ConnectionManager::new(client)).await {
                Ok(Ok(manager)) => manager,
                Ok(Err(e)) => panic!("Redis connection error: {}", e),
                Err(_) => panic!("Redis connection timed out after 5 seconds"),
            }
        });
        
        self.redis_connection = Some(conn_manager);
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
        
        // Load data from Redis using the global runtime
        if let Some(conn) = self.redis_connection.as_mut() {
            let runtime = get_global_redis_runtime();
            let result = runtime.block_on(async {
                self.table_type.load_data(conn, &self.table_key_prefix).await
            });
            if let Err(e) = result {
                pgrx::log!("Failed to load data from Redis: {:?}", e);
            }
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
            let runtime = get_global_redis_runtime();
            runtime.block_on(async {
                self.table_type.insert(conn, &self.table_key_prefix, data).await
            })
        } else {
            Err(redis::RedisError::from((redis::ErrorKind::IoError, "Redis connection not initialized")))
        }
    }

    /// Delete data using the appropriate table type
    pub fn delete_data(&mut self, data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(conn) = self.redis_connection.as_mut() {
            let runtime = get_global_redis_runtime();
            runtime.block_on(async {
                self.table_type.delete(conn, &self.table_key_prefix, data).await
            })
        } else {
            Err(redis::RedisError::from((redis::ErrorKind::IoError, "Redis connection not initialized")))
        }
    }

    /// Update data using the appropriate table type
    pub fn update_data(&mut self, old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError> {
        if let Some(conn) = self.redis_connection.as_mut() {
            let runtime = get_global_redis_runtime();
            runtime.block_on(async {
                self.table_type.update(conn, &self.table_key_prefix, old_data, new_data).await
            })
        } else {
            Err(redis::RedisError::from((redis::ErrorKind::IoError, "Redis connection not initialized")))
        }
    }

    /// Get a row at the specified index
    pub fn get_row(&self, index: usize) -> Option<Vec<String>> {
        self.table_type.get_row(index)
    }
}


