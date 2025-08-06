pub mod implementations;
/// Redis table type implementations module
///
/// This module contains all the Redis data type implementations that provide
/// Foreign Data Wrapper functionality for different Redis data structures.
pub mod interface;
pub mod macros;
pub mod types;

// Re-export the interface trait for convenience
pub use interface::RedisTableOperations;

// Re-export common types
pub use types::*;

// Re-export all table implementations
pub use implementations::*;
