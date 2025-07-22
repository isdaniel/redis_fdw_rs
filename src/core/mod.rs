/// Core FDW functionality module
///
/// This module contains the essential components for the Redis Foreign Data Wrapper:
/// - Connection management to Redis servers
/// - FDW handlers that integrate with PostgreSQL's foreign data wrapper infrastructure
/// - State management for query execution
pub mod connection;
pub mod handlers;
pub mod state;
