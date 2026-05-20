use crate::{
    auth::RedisAuthConfig,
    core::pool_manager::{get_pooled_connection, PoolConfig, PooledConnection},
};
/// Redis connection factory module
///
/// This module provides a clean interface for creating Redis connections
/// with proper error handling, configuration validation, and retry logic.
/// It supports both single-node and cluster Redis deployments with authentication.
///
/// The factory now uses a global connection pool manager for efficient connection
/// reuse across queries, significantly improving performance under concurrent workloads.
use pgrx::prelude::*;
use std::collections::HashMap;

/// Errors that can occur during connection creation
#[derive(Debug, thiserror::Error)]
pub enum ConnectionFactoryError {
    #[error("Invalid host_port configuration: {0}")]
    InvalidHostPort(String),

    #[error("Failed to create Redis client: {0}")]
    ClientCreationFailed(#[from] redis::RedisError),

    #[error("Failed to establish connection: {0}")]
    ConnectionFailed(String),

    #[error("Database parameter out of range: {0}")]
    InvalidDatabase(i64),

    #[error("Missing required configuration: {0}")]
    MissingConfiguration(String),

    #[error("Connection pool error: {0}")]
    PoolError(#[from] r2d2::Error),
}

pub type ConnectionFactoryResult<T> = Result<T, ConnectionFactoryError>;

/// Configuration for Redis connection creation
#[derive(Debug, Clone)]
pub struct RedisConnectionConfig {
    pub host_port: String,
    pub database: i64,
    pub retry_attempts: Option<u32>,
    pub auth_config: RedisAuthConfig,
    pub pool_config: PoolConfig,
}

impl RedisConnectionConfig {
    /// Create a new configuration from options map
    pub fn from_options(opts: &HashMap<String, String>) -> ConnectionFactoryResult<Self> {
        let host_port = opts
            .get("host_port")
            .ok_or_else(|| ConnectionFactoryError::MissingConfiguration("host_port".to_string()))?
            .clone();

        let database = opts
            .get("database")
            .map(|db_str| {
                db_str.parse::<i64>().map_err(|_| {
                    ConnectionFactoryError::InvalidDatabase(db_str.parse().unwrap_or(-1))
                })
            })
            .transpose()?
            .unwrap_or(0);

        let config = RedisConnectionConfig {
            host_port,
            database,
            retry_attempts: Some(3),
            auth_config: RedisAuthConfig::from_user_mapping_options(opts),
            pool_config: PoolConfig::from_options(opts),
        };

        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration
    fn validate(&self) -> ConnectionFactoryResult<()> {
        if self.host_port.trim().is_empty() {
            return Err(ConnectionFactoryError::InvalidHostPort(
                "Host port cannot be empty".to_string(),
            ));
        }

        if self.database < 0 || self.database > 15 {
            return Err(ConnectionFactoryError::InvalidDatabase(self.database));
        }

        Ok(())
    }
}

/// Redis connection factory for creating properly configured connections
pub struct RedisConnectionFactory;

impl RedisConnectionFactory {
    /// Create a connection using the global pool manager (recommended)
    /// This method reuses existing pools for the same configuration,
    /// significantly improving performance under concurrent workloads.
    pub fn create_pooled_connection(
        config: &RedisConnectionConfig,
    ) -> ConnectionFactoryResult<PooledConnection> {
        get_pooled_connection(
            &config.host_port,
            config.database,
            &config.auth_config,
            &config.pool_config,
        )
        .map_err(|e| ConnectionFactoryError::ConnectionFailed(e.to_string()))
    }

    /// Create a connection with retry logic using the global pool
    pub fn create_connection_with_retry(
        config: &RedisConnectionConfig,
    ) -> ConnectionFactoryResult<PooledConnection> {
        let retry_attempts = config.retry_attempts.unwrap_or(3);

        for attempt in 1..=retry_attempts {
            match Self::create_pooled_connection(config) {
                Ok(connection) => {
                    log!(
                        "Successfully acquired Redis connection from pool on attempt {}",
                        attempt
                    );
                    return Ok(connection);
                }
                Err(e) if attempt < retry_attempts => {
                    log!("Connection attempt {} failed, retrying: {}", attempt, e);
                    pgrx::check_for_interrupts!();
                }
                Err(e) => {
                    return Err(ConnectionFactoryError::ConnectionFailed(format!(
                        "Failed to connect after {} attempts: {}",
                        retry_attempts, e
                    )));
                }
            }
        }

        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_valid_options() {
        let mut opts = HashMap::new();
        opts.insert("host_port".to_string(), "127.0.0.1:6379".to_string());
        opts.insert("database".to_string(), "0".to_string());

        let config = RedisConnectionConfig::from_options(&opts).unwrap();
        assert_eq!(config.host_port, "127.0.0.1:6379");
        assert_eq!(config.database, 0);
    }

    #[test]
    fn test_config_default_database() {
        let mut opts = HashMap::new();
        opts.insert("host_port".to_string(), "127.0.0.1:6379".to_string());

        let config = RedisConnectionConfig::from_options(&opts).unwrap();
        assert_eq!(config.database, 0);
    }

    #[test]
    fn test_config_missing_host_port() {
        let opts = HashMap::new();
        let result = RedisConnectionConfig::from_options(&opts);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ConnectionFactoryError::MissingConfiguration(_)
        ));
    }

    #[test]
    fn test_config_invalid_database_too_high() {
        let mut opts = HashMap::new();
        opts.insert("host_port".to_string(), "127.0.0.1:6379".to_string());
        opts.insert("database".to_string(), "16".to_string());

        let result = RedisConnectionConfig::from_options(&opts);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ConnectionFactoryError::InvalidDatabase(16)
        ));
    }

    #[test]
    fn test_config_invalid_database_negative() {
        let mut opts = HashMap::new();
        opts.insert("host_port".to_string(), "127.0.0.1:6379".to_string());
        opts.insert("database".to_string(), "-1".to_string());

        let result = RedisConnectionConfig::from_options(&opts);
        assert!(result.is_err());
    }

    #[test]
    fn test_config_empty_host_port() {
        let mut opts = HashMap::new();
        opts.insert("host_port".to_string(), "   ".to_string());
        opts.insert("database".to_string(), "0".to_string());

        let result = RedisConnectionConfig::from_options(&opts);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ConnectionFactoryError::InvalidHostPort(_)
        ));
    }

    #[test]
    fn test_config_with_redis_prefix() {
        let mut opts = HashMap::new();
        opts.insert(
            "host_port".to_string(),
            "redis://127.0.0.1:6379".to_string(),
        );
        opts.insert("database".to_string(), "5".to_string());

        let config = RedisConnectionConfig::from_options(&opts).unwrap();
        assert_eq!(config.host_port, "redis://127.0.0.1:6379");
        assert_eq!(config.database, 5);
    }

    #[test]
    fn test_config_database_boundary_values() {
        for db in 0..=15 {
            let mut opts = HashMap::new();
            opts.insert("host_port".to_string(), "127.0.0.1:6379".to_string());
            opts.insert("database".to_string(), db.to_string());
            assert!(RedisConnectionConfig::from_options(&opts).is_ok());
        }
    }
}
