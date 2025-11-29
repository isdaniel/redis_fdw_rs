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
use std::time::Duration;

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

    #[error("Configuration validation failed: {0}")]
    ValidationFailed(String),

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
    pub retry_delay: Option<Duration>,
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
            retry_delay: Some(Duration::from_millis(100)),
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

    /// Check if this configuration is for cluster mode
    pub fn is_cluster_mode(&self) -> bool {
        self.host_port.contains(',')
    }

    /// Parse host_port into individual node URLs for cluster mode
    pub fn parse_cluster_nodes(&self) -> ConnectionFactoryResult<Vec<String>> {
        if !self.is_cluster_mode() {
            return Err(ConnectionFactoryError::ValidationFailed(
                "Not a cluster configuration".to_string(),
            ));
        }

        let nodes: Result<Vec<String>, _> = self
            .host_port
            .split(',')
            .map(|node| {
                let trimmed = node.trim();
                if trimmed.is_empty() {
                    return Err(ConnectionFactoryError::InvalidHostPort(
                        "Empty node in cluster configuration".to_string(),
                    ));
                }

                // Create base URL with database
                let base_url = if trimmed.starts_with("redis://") {
                    format!("{}/{}", trimmed, self.database)
                } else {
                    format!("redis://{}/{}", trimmed, self.database)
                };

                // Apply authentication if required
                let url = self.auth_config.apply_to_url(&base_url);

                Ok(url)
            })
            .collect();

        nodes
    }

    /// Get the single node URL for non-cluster mode
    pub fn get_single_node_url(&self) -> ConnectionFactoryResult<String> {
        if self.is_cluster_mode() {
            return Err(ConnectionFactoryError::ValidationFailed(
                "Cannot get single node URL for cluster configuration".to_string(),
            ));
        }

        // Create base URL with database
        let base_url = if self.host_port.starts_with("redis://") {
            format!("{}/{}", self.host_port, self.database)
        } else {
            format!("redis://{}/{}", self.host_port, self.database)
        };

        // Apply authentication if required
        let url = self.auth_config.apply_to_url(&base_url);

        Ok(url)
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
        let retry_delay = config.retry_delay.unwrap_or(Duration::from_millis(100));

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
                    log!(
                        "Connection attempt {} failed, retrying in {:?}: {}",
                        attempt,
                        retry_delay,
                        e
                    );
                    std::thread::sleep(retry_delay);
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
