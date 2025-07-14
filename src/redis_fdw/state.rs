use std::collections::HashMap;
use pgrx::pg_sys::{Oid, Datum, MemoryContext};
pub struct RedisFdwState {
    pub tmp_ctx: MemoryContext,
    pub header_name_to_colno: HashMap<String, usize>,
    pub redis_connection: Option<redis::Connection>,
    pub table_type : String,
    pub table_key_prefix : String,
    pub is_read : bool
}

impl RedisFdwState {
    pub fn new(tmp_ctx: MemoryContext) -> Self {
        RedisFdwState {
            tmp_ctx,
            header_name_to_colno: HashMap::default(),
            redis_connection: None,
            table_type: String::default(),
            table_key_prefix: "*".to_string(),
            is_read : false
        }
    }
}

pub struct RedisModifyFdwState {
    pub tmp_ctx: MemoryContext,
    pub redis_connection: Option<redis::Connection>,
    pub table_type : String,
    pub table_key_prefix : String,
    pub opts: HashMap<String, String>
}

impl RedisModifyFdwState {
    pub fn new(tmp_ctx: MemoryContext) -> Self {
        RedisModifyFdwState {
            tmp_ctx,
            redis_connection: None,
            table_type: String::default(),
            table_key_prefix: "*".to_string(),
            opts: HashMap::default()
        }
    }
}





