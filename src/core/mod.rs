/// Core FDW functionality module
///
/// This module contains the essential components for the Redis Foreign Data Wrapper:
/// - Connection management to Redis servers
/// - Connection factory for creating and configuring Redis connections
/// - FDW handlers that integrate with PostgreSQL's foreign data wrapper infrastructure
/// - State management for query execution
/// - Data loading with optimization support
pub mod connection;
pub mod connection_factory;
pub mod data_loader;
pub mod handlers;
pub mod state_manager;
