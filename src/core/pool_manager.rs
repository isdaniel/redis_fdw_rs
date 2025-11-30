/// Global connection pool manager for Redis FDW
///
/// This module provides a global, thread-safe connection pool manager that caches
/// connection pools by their connection configuration. This eliminates the overhead
/// of creating new pools for every query, significantly improving performance under
/// concurrent workloads.
use crate::auth::RedisAuthConfig;
use redis::{cluster::ClusterClient, Client};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// Global singleton for connection pools
static POOL_MANAGER: OnceLock<Mutex<PoolManager>> = OnceLock::new();

/// Configuration for connection pool behavior
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of connections in the pool
    pub max_size: u32,
    /// Minimum number of idle connections to maintain
    pub min_idle: Option<u32>,
    /// Timeout for acquiring a connection from the pool
    pub connection_timeout: Duration,
    /// Maximum lifetime of a connection before it's closed and replaced
    pub max_lifetime: Option<Duration>,
    /// Idle timeout before a connection is closed
    pub idle_timeout: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 64,
            min_idle: Some(8),
            connection_timeout: Duration::from_secs(30),
            max_lifetime: Some(Duration::from_secs(1800)), // 30 minutes
            idle_timeout: Some(Duration::from_secs(600)),  // 10 minutes
        }
    }
}

impl PoolConfig {
    /// Create configuration from options map
    pub fn from_options(opts: &HashMap<String, String>) -> Self {
        let mut config = Self::default();

        if let Some(max_size) = opts.get("pool_max_size") {
            if let Ok(size) = max_size.parse::<u32>() {
                config.max_size = size.clamp(1, 512);
            }
        }

        if let Some(min_idle) = opts.get("pool_min_idle") {
            if let Ok(idle) = min_idle.parse::<u32>() {
                config.min_idle = Some(idle.clamp(0, config.max_size));
            }
        }

        if let Some(timeout) = opts.get("pool_connection_timeout_ms") {
            if let Ok(ms) = timeout.parse::<u64>() {
                config.connection_timeout = Duration::from_millis(ms.clamp(100, 60000));
            }
        }

        if let Some(lifetime) = opts.get("pool_max_lifetime_secs") {
            if let Ok(secs) = lifetime.parse::<u64>() {
                config.max_lifetime = Some(Duration::from_secs(secs.clamp(60, 7200)));
            }
        }

        if let Some(idle_timeout) = opts.get("pool_idle_timeout_secs") {
            if let Ok(secs) = idle_timeout.parse::<u64>() {
                config.idle_timeout = Some(Duration::from_secs(secs.clamp(30, 3600)));
            }
        }

        config
    }
}

/// Pool types for different Redis configurations
pub enum RedisPool {
    Single(r2d2::Pool<Client>),
    Cluster(r2d2::Pool<ClusterClient>),
}

/// Cached pool entry with metadata
struct PoolEntry {
    pool: RedisPool,
}

/// Global pool manager that caches connection pools by configuration key
pub struct PoolManager {
    single_pools: HashMap<String, PoolEntry>,
    cluster_pools: HashMap<String, PoolEntry>,
}

impl PoolManager {
    fn new() -> Self {
        Self {
            single_pools: HashMap::new(),
            cluster_pools: HashMap::new(),
        }
    }

    /// Get the global pool manager instance
    pub fn global() -> &'static Mutex<PoolManager> {
        POOL_MANAGER.get_or_init(|| Mutex::new(PoolManager::new()))
    }

    /// Generate a cache key for single-node configuration
    fn single_pool_key(host_port: &str, database: i64, auth_config: &RedisAuthConfig) -> String {
        format!(
            "single:{}:{}:{}",
            host_port,
            database,
            auth_config.cache_key()
        )
    }

    /// Generate a cache key for cluster configuration
    fn cluster_pool_key(host_port: &str, database: i64, auth_config: &RedisAuthConfig) -> String {
        format!(
            "cluster:{}:{}:{}",
            host_port,
            database,
            auth_config.cache_key()
        )
    }

    /// Get or create a single-node Redis pool
    pub fn get_or_create_single_pool(
        &mut self,
        host_port: &str,
        database: i64,
        auth_config: &RedisAuthConfig,
        pool_config: &PoolConfig,
    ) -> Result<r2d2::Pool<Client>, PoolError> {
        let key = Self::single_pool_key(host_port, database, auth_config);

        if let Some(entry) = self.single_pools.get(&key) {
            if let RedisPool::Single(pool) = &entry.pool {
                return Ok(pool.clone());
            }
        }

        // Create new pool
        let url = build_single_url(host_port, database, auth_config)?;
        let client = Client::open(url).map_err(|e| PoolError::ClientCreation(e.to_string()))?;

        let mut builder = r2d2::Pool::builder()
            .max_size(pool_config.max_size)
            .connection_timeout(pool_config.connection_timeout);

        if let Some(min_idle) = pool_config.min_idle {
            builder = builder.min_idle(Some(min_idle));
        }

        if let Some(max_lifetime) = pool_config.max_lifetime {
            builder = builder.max_lifetime(Some(max_lifetime));
        }

        if let Some(idle_timeout) = pool_config.idle_timeout {
            builder = builder.idle_timeout(Some(idle_timeout));
        }

        let pool = builder
            .build(client)
            .map_err(|e| PoolError::PoolCreation(e.to_string()))?;

        // Cache the pool
        self.single_pools.insert(
            key,
            PoolEntry {
                pool: RedisPool::Single(pool.clone()),
            },
        );

        Ok(pool)
    }

    /// Get or create a cluster Redis pool
    pub fn get_or_create_cluster_pool(
        &mut self,
        host_port: &str,
        database: i64,
        auth_config: &RedisAuthConfig,
        pool_config: &PoolConfig,
    ) -> Result<r2d2::Pool<ClusterClient>, PoolError> {
        let key = Self::cluster_pool_key(host_port, database, auth_config);

        if let Some(entry) = self.cluster_pools.get(&key) {
            if let RedisPool::Cluster(pool) = &entry.pool {
                return Ok(pool.clone());
            }
        }

        // Create new pool
        let nodes = build_cluster_urls(host_port, database, auth_config)?;
        let client =
            ClusterClient::new(nodes).map_err(|e| PoolError::ClientCreation(e.to_string()))?;

        let mut builder = r2d2::Pool::builder()
            .max_size(pool_config.max_size)
            .connection_timeout(pool_config.connection_timeout);

        if let Some(min_idle) = pool_config.min_idle {
            builder = builder.min_idle(Some(min_idle));
        }

        if let Some(max_lifetime) = pool_config.max_lifetime {
            builder = builder.max_lifetime(Some(max_lifetime));
        }

        if let Some(idle_timeout) = pool_config.idle_timeout {
            builder = builder.idle_timeout(Some(idle_timeout));
        }

        let pool = builder
            .build(client)
            .map_err(|e| PoolError::PoolCreation(e.to_string()))?;

        // Cache the pool
        self.cluster_pools.insert(
            key,
            PoolEntry {
                pool: RedisPool::Cluster(pool.clone()),
            },
        );

        Ok(pool)
    }
}

/// Errors that can occur in pool management
#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("Failed to create Redis client: {0}")]
    ClientCreation(String),

    #[error("Failed to create connection pool: {0}")]
    PoolCreation(String),

    #[error("Failed to get connection from pool: {0}")]
    ConnectionAcquisition(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Pool manager lock poisoned")]
    LockPoisoned,
}

/// Build URL for single-node connection
fn build_single_url(
    host_port: &str,
    database: i64,
    auth_config: &RedisAuthConfig,
) -> Result<String, PoolError> {
    let base_url = if host_port.starts_with("redis://") {
        format!("{}/{}", host_port, database)
    } else {
        format!("redis://{}/{}", host_port, database)
    };

    Ok(auth_config.apply_to_url(&base_url))
}

/// Build URLs for cluster nodes
fn build_cluster_urls(
    host_port: &str,
    database: i64,
    auth_config: &RedisAuthConfig,
) -> Result<Vec<String>, PoolError> {
    host_port
        .split(',')
        .map(|node| {
            let trimmed = node.trim();
            if trimmed.is_empty() {
                return Err(PoolError::InvalidConfig(
                    "Empty node in cluster configuration".to_string(),
                ));
            }

            let base_url = if trimmed.starts_with("redis://") {
                format!("{}/{}", trimmed, database)
            } else {
                format!("redis://{}/{}", trimmed, database)
            };

            Ok(auth_config.apply_to_url(&base_url))
        })
        .collect()
}

/// High-level helper to get a connection from the global pool
pub fn get_pooled_connection(
    host_port: &str,
    database: i64,
    auth_config: &RedisAuthConfig,
    pool_config: &PoolConfig,
) -> Result<PooledConnection, PoolError> {
    let is_cluster = host_port.contains(',');

    let manager = PoolManager::global();
    let mut manager = manager.lock().map_err(|_| PoolError::LockPoisoned)?;

    if is_cluster {
        let pool =
            manager.get_or_create_cluster_pool(host_port, database, auth_config, pool_config)?;
        let conn = pool
            .get()
            .map_err(|e| PoolError::ConnectionAcquisition(e.to_string()))?;
        Ok(PooledConnection::Cluster(conn))
    } else {
        let pool =
            manager.get_or_create_single_pool(host_port, database, auth_config, pool_config)?;
        let conn = pool
            .get()
            .map_err(|e| PoolError::ConnectionAcquisition(e.to_string()))?;
        Ok(PooledConnection::Single(conn))
    }
}

/// Wrapper for pooled connections that implements ConnectionLike access
pub enum PooledConnection {
    Single(r2d2::PooledConnection<Client>),
    Cluster(r2d2::PooledConnection<ClusterClient>),
}

impl PooledConnection {
    pub fn as_connection_like_mut(&mut self) -> &mut dyn redis::ConnectionLike {
        match self {
            PooledConnection::Single(conn) => conn,
            PooledConnection::Cluster(conn) => conn,
        }
    }
}
