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
#[derive(Debug, Clone, PartialEq)]
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

    /// Apply this configuration to an r2d2 pool builder
    fn apply_to_builder<M: r2d2::ManageConnection>(
        &self,
        builder: r2d2::Builder<M>,
    ) -> r2d2::Builder<M> {
        let mut builder = builder
            .max_size(self.max_size)
            .connection_timeout(self.connection_timeout);

        if let Some(min_idle) = self.min_idle {
            builder = builder.min_idle(Some(min_idle));
        }

        if let Some(max_lifetime) = self.max_lifetime {
            builder = builder.max_lifetime(Some(max_lifetime));
        }

        if let Some(idle_timeout) = self.idle_timeout {
            builder = builder.idle_timeout(Some(idle_timeout));
        }

        builder
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

// ============================================================================
// URL Building Utilities
// ============================================================================

/// Build a Redis URL from host:port with optional database and auth
fn build_redis_url(host_port: &str, database: i64, auth_config: &RedisAuthConfig) -> String {
    let base_url = if host_port.starts_with("redis://") {
        format!("{}/{}", host_port, database)
    } else {
        format!("redis://{}/{}", host_port, database)
    };
    auth_config.apply_to_url(&base_url)
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
            Ok(build_redis_url(trimmed, database, auth_config))
        })
        .collect()
}

/// Determine the connection type based on host_port format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisConnectionType {
    /// Single Redis node (no comma in host_port)
    Single,
    /// Redis Cluster (comma-separated nodes in host_port)
    Cluster,
}

impl RedisConnectionType {
    /// Detect connection type from host_port string
    pub fn from_host_port(host_port: &str) -> Self {
        if host_port.contains(',') {
            Self::Cluster
        } else {
            Self::Single
        }
    }

    /// Get the pool type prefix for cache key generation
    fn cache_key_prefix(&self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Cluster => "cluster",
        }
    }
}

/// Trait for creating Redis connection pools with unified configuration handling
///
/// This trait abstracts the common pool creation logic between single-node and
/// cluster Redis deployments, ensuring consistent configuration application
/// and reducing code duplication.
pub trait RedisPoolProvider: r2d2::ManageConnection + Sized {
    /// Create the Redis client from configuration parameters
    fn create_client(
        host_port: &str,
        database: i64,
        auth_config: &RedisAuthConfig,
    ) -> Result<Self, PoolError>;

    /// Get the connection type for this provider
    fn connection_type() -> RedisConnectionType;

    /// Create a connection pool with the given configuration
    fn create_pool(
        host_port: &str,
        database: i64,
        auth_config: &RedisAuthConfig,
        pool_config: &PoolConfig,
    ) -> Result<r2d2::Pool<Self>, PoolError> {
        let client = Self::create_client(host_port, database, auth_config)?;
        let builder = pool_config.apply_to_builder(r2d2::Pool::builder());
        builder
            .build(client)
            .map_err(|e| PoolError::PoolCreation(e.to_string()))
    }

    /// Generate a cache key for pool identification
    fn cache_key(host_port: &str, database: i64, auth_config: &RedisAuthConfig) -> String {
        format!(
            "{}:{}:{}:{}",
            Self::connection_type().cache_key_prefix(),
            host_port,
            database,
            auth_config.cache_key()
        )
    }
}

/// Implementation for single-node Redis client
impl RedisPoolProvider for Client {
    fn create_client(
        host_port: &str,
        database: i64,
        auth_config: &RedisAuthConfig,
    ) -> Result<Self, PoolError> {
        let url = build_redis_url(host_port, database, auth_config);
        Client::open(url).map_err(|e| PoolError::ClientCreation(e.to_string()))
    }

    fn connection_type() -> RedisConnectionType {
        RedisConnectionType::Single
    }
}

/// Implementation for Redis Cluster client
impl RedisPoolProvider for ClusterClient {
    fn create_client(
        host_port: &str,
        database: i64,
        auth_config: &RedisAuthConfig,
    ) -> Result<Self, PoolError> {
        let nodes = build_cluster_urls(host_port, database, auth_config)?;
        ClusterClient::new(nodes).map_err(|e| PoolError::ClientCreation(e.to_string()))
    }

    fn connection_type() -> RedisConnectionType {
        RedisConnectionType::Cluster
    }
}

/// Global pool manager that caches connection pools by configuration key
///
/// Uses a unified storage approach with type-safe pool retrieval through
/// the `RedisPoolProvider` trait.
pub struct PoolManager {
    /// Cache for single-node Redis pools
    single_pools: HashMap<String, r2d2::Pool<Client>>,
    /// Cache for cluster Redis pools
    cluster_pools: HashMap<String, r2d2::Pool<ClusterClient>>,
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

    /// Get or create a single-node Redis pool
    pub fn get_or_create_single_pool(
        &mut self,
        host_port: &str,
        database: i64,
        auth_config: &RedisAuthConfig,
        pool_config: &PoolConfig,
    ) -> Result<r2d2::Pool<Client>, PoolError> {
        let key = Client::cache_key(host_port, database, auth_config);

        if let Some(pool) = self.single_pools.get(&key) {
            return Ok(pool.clone());
        }

        let pool = Client::create_pool(host_port, database, auth_config, pool_config)?;
        self.single_pools.insert(key, pool.clone());
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
        let key = ClusterClient::cache_key(host_port, database, auth_config);

        if let Some(pool) = self.cluster_pools.get(&key) {
            return Ok(pool.clone());
        }

        let pool = ClusterClient::create_pool(host_port, database, auth_config, pool_config)?;
        self.cluster_pools.insert(key, pool.clone());
        Ok(pool)
    }

    /// Get or create a pool based on connection type auto-detection
    ///
    /// This is the recommended method for external callers who want automatic
    /// detection of single vs cluster mode based on the host_port format.
    pub fn get_or_create_pool(
        &mut self,
        host_port: &str,
        database: i64,
        auth_config: &RedisAuthConfig,
        pool_config: &PoolConfig,
    ) -> Result<RedisPool, PoolError> {
        match RedisConnectionType::from_host_port(host_port) {
            RedisConnectionType::Single => {
                let pool =
                    self.get_or_create_single_pool(host_port, database, auth_config, pool_config)?;
                Ok(RedisPool::Single(pool))
            }
            RedisConnectionType::Cluster => {
                let pool =
                    self.get_or_create_cluster_pool(host_port, database, auth_config, pool_config)?;
                Ok(RedisPool::Cluster(pool))
            }
        }
    }

    /// Get the number of cached single-node pools (for testing/monitoring)
    #[cfg(any(test, feature = "pg_test"))]
    pub fn single_pool_count(&self) -> usize {
        self.single_pools.len()
    }

    /// Get the number of cached cluster pools (for testing/monitoring)
    #[cfg(any(test, feature = "pg_test"))]
    pub fn cluster_pool_count(&self) -> usize {
        self.cluster_pools.len()
    }

    /// Clear all cached pools (useful for testing)
    #[cfg(any(test, feature = "pg_test"))]
    pub fn clear_all(&mut self) {
        self.single_pools.clear();
        self.cluster_pools.clear();
    }
}

/// Pool types for different Redis configurations
pub enum RedisPool {
    Single(r2d2::Pool<Client>),
    Cluster(r2d2::Pool<ClusterClient>),
}

impl RedisPool {
    /// Get a connection from the pool
    pub fn get_connection(&self) -> Result<PooledConnection, PoolError> {
        match self {
            RedisPool::Single(pool) => {
                let conn = pool
                    .get()
                    .map_err(|e| PoolError::ConnectionAcquisition(e.to_string()))?;
                Ok(PooledConnection::Single(conn))
            }
            RedisPool::Cluster(pool) => {
                let conn = pool
                    .get()
                    .map_err(|e| PoolError::ConnectionAcquisition(e.to_string()))?;
                Ok(PooledConnection::Cluster(conn))
            }
        }
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

/// High-level helper to get a connection from the global pool
///
/// Automatically detects single vs cluster mode based on the host_port format:
/// - Single node: "host:port" (e.g., "127.0.0.1:6379")
/// - Cluster: "host1:port1,host2:port2,..." (comma-separated)
pub fn get_pooled_connection(
    host_port: &str,
    database: i64,
    auth_config: &RedisAuthConfig,
    pool_config: &PoolConfig,
) -> Result<PooledConnection, PoolError> {
    let manager = PoolManager::global();
    let mut manager = manager.lock().map_err(|_| PoolError::LockPoisoned)?;

    let pool = manager.get_or_create_pool(host_port, database, auth_config, pool_config)?;
    pool.get_connection()
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[allow(unused_imports)]
mod tests {
    use super::*;

    // --------------------------------
    // PoolConfig Tests
    // --------------------------------

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_size, 64);
        assert_eq!(config.min_idle, Some(8));
        assert_eq!(config.connection_timeout, Duration::from_secs(30));
        assert_eq!(config.max_lifetime, Some(Duration::from_secs(1800)));
        assert_eq!(config.idle_timeout, Some(Duration::from_secs(600)));
    }

    #[test]
    fn test_pool_config_from_options() {
        let mut opts = HashMap::new();
        opts.insert("pool_max_size".to_string(), "128".to_string());
        opts.insert("pool_min_idle".to_string(), "16".to_string());
        opts.insert("pool_connection_timeout_ms".to_string(), "5000".to_string());
        opts.insert("pool_max_lifetime_secs".to_string(), "3600".to_string());
        opts.insert("pool_idle_timeout_secs".to_string(), "300".to_string());

        let config = PoolConfig::from_options(&opts);

        assert_eq!(config.max_size, 128);
        assert_eq!(config.min_idle, Some(16));
        assert_eq!(config.connection_timeout, Duration::from_millis(5000));
        assert_eq!(config.max_lifetime, Some(Duration::from_secs(3600)));
        assert_eq!(config.idle_timeout, Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_pool_config_from_options_clamps_values() {
        let mut opts = HashMap::new();
        opts.insert("pool_max_size".to_string(), "1000".to_string()); // > 512
        opts.insert("pool_min_idle".to_string(), "1000".to_string()); // > max_size
        opts.insert("pool_connection_timeout_ms".to_string(), "1".to_string()); // < 100
        opts.insert("pool_max_lifetime_secs".to_string(), "10".to_string()); // < 60
        opts.insert("pool_idle_timeout_secs".to_string(), "10".to_string()); // < 30

        let config = PoolConfig::from_options(&opts);

        assert_eq!(config.max_size, 512); // clamped
        assert_eq!(config.min_idle, Some(512)); // clamped to max_size
        assert_eq!(config.connection_timeout, Duration::from_millis(100)); // clamped
        assert_eq!(config.max_lifetime, Some(Duration::from_secs(60))); // clamped
        assert_eq!(config.idle_timeout, Some(Duration::from_secs(30))); // clamped
    }

    #[test]
    fn test_pool_config_from_options_ignores_invalid() {
        let mut opts = HashMap::new();
        opts.insert("pool_max_size".to_string(), "invalid".to_string());
        opts.insert("pool_min_idle".to_string(), "abc".to_string());

        let config = PoolConfig::from_options(&opts);
        let default = PoolConfig::default();

        // Should fall back to defaults for invalid values
        assert_eq!(config.max_size, default.max_size);
        assert_eq!(config.min_idle, default.min_idle);
    }

    #[test]
    fn test_pool_config_from_empty_options() {
        let opts = HashMap::new();
        let config = PoolConfig::from_options(&opts);
        let default = PoolConfig::default();

        assert_eq!(config, default);
    }

    // --------------------------------
    // RedisConnectionType Tests
    // --------------------------------

    #[test]
    fn test_connection_type_from_host_port() {
        // Single node
        assert_eq!(
            RedisConnectionType::from_host_port("127.0.0.1:6379"),
            RedisConnectionType::Single
        );
        assert_eq!(
            RedisConnectionType::from_host_port("redis://localhost:6379"),
            RedisConnectionType::Single
        );

        // Cluster
        assert_eq!(
            RedisConnectionType::from_host_port("127.0.0.1:7000,127.0.0.1:7001"),
            RedisConnectionType::Cluster
        );
        assert_eq!(
            RedisConnectionType::from_host_port("node1:6379,node2:6379,node3:6379"),
            RedisConnectionType::Cluster
        );
    }

    #[test]
    fn test_connection_type_cache_key_prefix() {
        assert_eq!(RedisConnectionType::Single.cache_key_prefix(), "single");
        assert_eq!(RedisConnectionType::Cluster.cache_key_prefix(), "cluster");
    }

    // --------------------------------
    // URL Building Tests
    // --------------------------------

    #[test]
    fn test_build_redis_url_simple() {
        let auth = RedisAuthConfig::default();
        let url = build_redis_url("127.0.0.1:6379", 0, &auth);
        assert_eq!(url, "redis://127.0.0.1:6379/0");
    }

    #[test]
    fn test_build_redis_url_with_database() {
        let auth = RedisAuthConfig::default();
        let url = build_redis_url("localhost:6379", 5, &auth);
        assert_eq!(url, "redis://localhost:6379/5");
    }

    #[test]
    fn test_build_redis_url_with_scheme() {
        let auth = RedisAuthConfig::default();
        let url = build_redis_url("redis://myhost:6379", 3, &auth);
        assert_eq!(url, "redis://myhost:6379/3");
    }

    #[test]
    fn test_build_redis_url_with_password() {
        let auth = RedisAuthConfig {
            password: Some("secret123".to_string()),
            username: None,
        };
        let url = build_redis_url("127.0.0.1:6379", 0, &auth);
        assert!(url.contains(":secret123@"));
        assert!(url.starts_with("redis://"));
        assert!(url.ends_with("/0"));
    }

    #[test]
    fn test_build_redis_url_with_username_and_password() {
        let auth = RedisAuthConfig {
            password: Some("mypass".to_string()),
            username: Some("myuser".to_string()),
        };
        let url = build_redis_url("127.0.0.1:6379", 0, &auth);
        assert!(url.contains("myuser:mypass@"));
    }

    #[test]
    fn test_build_cluster_urls() {
        let auth = RedisAuthConfig::default();
        let urls =
            build_cluster_urls("127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002", 0, &auth).unwrap();

        assert_eq!(urls.len(), 3);
        assert_eq!(urls[0], "redis://127.0.0.1:7000/0");
        assert_eq!(urls[1], "redis://127.0.0.1:7001/0");
        assert_eq!(urls[2], "redis://127.0.0.1:7002/0");
    }

    #[test]
    fn test_build_cluster_urls_with_spaces() {
        let auth = RedisAuthConfig::default();
        let urls = build_cluster_urls("127.0.0.1:7000 , 127.0.0.1:7001 , 127.0.0.1:7002", 0, &auth)
            .unwrap();

        assert_eq!(urls.len(), 3);
        assert_eq!(urls[0], "redis://127.0.0.1:7000/0");
        assert_eq!(urls[1], "redis://127.0.0.1:7001/0");
        assert_eq!(urls[2], "redis://127.0.0.1:7002/0");
    }

    #[test]
    fn test_build_cluster_urls_empty_node_error() {
        let auth = RedisAuthConfig::default();
        let result = build_cluster_urls("127.0.0.1:7000,,127.0.0.1:7002", 0, &auth);

        assert!(result.is_err());
        if let Err(PoolError::InvalidConfig(msg)) = result {
            assert!(msg.contains("Empty node"));
        } else {
            panic!("Expected InvalidConfig error");
        }
    }

    #[test]
    fn test_build_cluster_urls_with_auth() {
        let auth = RedisAuthConfig {
            password: Some("clusterpass".to_string()),
            username: None,
        };
        let urls = build_cluster_urls("node1:7000,node2:7001", 0, &auth).unwrap();

        assert_eq!(urls.len(), 2);
        assert!(urls[0].contains(":clusterpass@"));
        assert!(urls[1].contains(":clusterpass@"));
    }

    // --------------------------------
    // Cache Key Tests
    // --------------------------------

    #[test]
    fn test_single_pool_cache_key() {
        let auth = RedisAuthConfig::default();
        let key = Client::cache_key("127.0.0.1:6379", 0, &auth);

        assert!(key.starts_with("single:"));
        assert!(key.contains("127.0.0.1:6379"));
        assert!(key.contains(":0:"));
    }

    #[test]
    fn test_cluster_pool_cache_key() {
        let auth = RedisAuthConfig::default();
        let key = ClusterClient::cache_key("127.0.0.1:7000,127.0.0.1:7001", 0, &auth);

        assert!(key.starts_with("cluster:"));
        assert!(key.contains("127.0.0.1:7000,127.0.0.1:7001"));
    }

    #[test]
    fn test_cache_key_differs_by_auth() {
        let no_auth = RedisAuthConfig::default();
        let with_auth = RedisAuthConfig {
            password: Some("secret".to_string()),
            username: None,
        };

        let key1 = Client::cache_key("127.0.0.1:6379", 0, &no_auth);
        let key2 = Client::cache_key("127.0.0.1:6379", 0, &with_auth);

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_cache_key_differs_by_database() {
        let auth = RedisAuthConfig::default();

        let key1 = Client::cache_key("127.0.0.1:6379", 0, &auth);
        let key2 = Client::cache_key("127.0.0.1:6379", 1, &auth);

        assert_ne!(key1, key2);
    }

    // --------------------------------
    // PoolManager Tests
    // --------------------------------

    #[test]
    fn test_pool_manager_new() {
        let manager = PoolManager::new();
        assert_eq!(manager.single_pool_count(), 0);
        assert_eq!(manager.cluster_pool_count(), 0);
    }

    #[test]
    fn test_pool_manager_clear_all() {
        let mut manager = PoolManager::new();
        // Can't actually create pools without Redis, but we can test the clear
        manager.clear_all();
        assert_eq!(manager.single_pool_count(), 0);
        assert_eq!(manager.cluster_pool_count(), 0);
    }

    // --------------------------------
    // PoolError Tests
    // --------------------------------

    #[test]
    fn test_pool_error_display() {
        let err = PoolError::ClientCreation("test error".to_string());
        assert!(err.to_string().contains("test error"));

        let err = PoolError::PoolCreation("pool error".to_string());
        assert!(err.to_string().contains("pool error"));

        let err = PoolError::ConnectionAcquisition("conn error".to_string());
        assert!(err.to_string().contains("conn error"));

        let err = PoolError::InvalidConfig("config error".to_string());
        assert!(err.to_string().contains("config error"));

        let err = PoolError::LockPoisoned;
        assert!(err.to_string().contains("poisoned"));
    }
}
