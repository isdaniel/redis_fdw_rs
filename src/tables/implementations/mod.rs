/// Redis table implementations module
///
/// This module contains the actual implementations of different Redis data types
/// as PostgreSQL foreign tables, with each data type having its own dedicated module.
pub mod hash;
pub mod list;
pub mod set;
pub mod stream;
pub mod string;
pub mod zset;

// Re-export all table types for convenience
pub use hash::RedisHashTable;
pub use list::RedisListTable;
pub use set::RedisSetTable;
pub use stream::RedisStreamTable;
pub use string::RedisStringTable;
pub use zset::RedisZSetTable;
