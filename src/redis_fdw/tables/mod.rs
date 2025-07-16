pub mod interface;
pub mod redis_hash_table;
pub mod redis_list_table;
pub mod redis_set_table;
pub mod redis_string_table;
pub mod redis_zset_table;

// Re-export the interface trait for convenience
pub use interface::RedisTableOperations;

// Re-export all table types
pub use redis_hash_table::RedisHashTable;
pub use redis_list_table::RedisListTable;
pub use redis_set_table::RedisSetTable;
pub use redis_string_table::RedisStringTable;
pub use redis_zset_table::RedisZSetTable;
