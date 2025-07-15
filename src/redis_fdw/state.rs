use std::collections::HashMap;
use pgrx::pg_sys::MemoryContext;
use redis::Commands;

pub enum RedisTableType {
    String,
    Hash,
    List,
    Set,
    ZSet,
    None
}

impl RedisTableType {
    pub fn from_str(s: &str) -> RedisTableType {
        match s.to_lowercase().as_str() {
            "string" => RedisTableType::String,
            "hash" => RedisTableType::Hash,
            "list" => RedisTableType::List,
            "set" => RedisTableType::Set,
            "zset" => RedisTableType::ZSet,
            _ => RedisTableType::None,
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
    pub data: Vec<(String, String)>,
    pub row_count: u32,
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
            data: Vec::new(), 
            row_count: 0,
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

        self.table_type = RedisTableType::from_str(
            self.opts
                .get("table_type")
                .expect("`table_type` option is required for redis_fdw"),
        );

        if let Some(db_str) = self.opts.get("database") {
            self.database = db_str
                .parse::<i64>()
                .unwrap_or_else(|_| panic!("Invalid `database` value: {}", db_str));
        }

        if let Some(prefix) = self.opts.get("table_key_prefix") {
            self.table_key_prefix = prefix.clone();
        }
    }
    //SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
    pub fn hash_hgetall(&mut self) {
        let conn = self.redis_connection.as_mut().expect("Redis connection is not initialized");
        let table_key_prefix = &self.table_key_prefix;
        self.data = conn.hgetall(table_key_prefix).map(|map: HashMap<String, String>| {
            map.into_iter().collect()
        }).expect("Failed to get Redis hash data");
    } 

    pub fn hash_hset_multiple(&mut self, fields: &[(String, String)]) {
        let conn: &mut redis::Connection = self.redis_connection.as_mut().expect("Redis connection is not initialized");
        let table_key_prefix = &self.table_key_prefix;
        let _: () = conn.hset_multiple(table_key_prefix, fields).expect("Failed to set Redis hash field");
    }

    pub fn is_read_end(&self) -> bool {
        self.row_count >= self.data.len() as u32
    }
}

///// Write FDW state (for INSERT/UPDATE/DELETE)
// pub struct RedisModifyFdwState {
//     pub base: RedisBaseState
// }

// impl RedisModifyFdwState {
//     pub fn new(tmp_ctx: MemoryContext) -> Self {
//         RedisModifyFdwState {
//             base: RedisBaseState {
//                 tmp_ctx,
//                 redis_connection: None,
//                 table_type: String::default(),
//                 table_key_prefix: String::default(),
//                 database: 0,
//                 host_port: String::default(),
//                 opts: HashMap::default(),
//             }
//         }
//     }
// }

