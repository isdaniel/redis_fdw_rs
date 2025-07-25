use crate::core::connection::RedisConnectionType;
/// Redis connection factory module
///
/// This module provides a clean interface for creating Redis connections
/// with proper error handling, configuration validation, and retry logic.
/// It supports both single-node and cluster Redis deployments with advanced
/// jitter retry strategies to handle network instability gracefully.
use pgrx::prelude::*;
use redis::{cluster::ClusterClient, Client};
use rand::Rng;
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
}

pub type ConnectionFactoryResult<T> = Result<T, ConnectionFactoryError>;

/// Jitter strategy for retry backoff
#[derive(Debug, Clone, Copy)]
pub enum JitterStrategy {
    /// No jitter - use fixed delays
    None,
    /// Full jitter - random delay between 0 and calculated backoff
    Full,
    /// Equal jitter - half fixed delay + half random delay
    Equal,
    /// Decorrelated jitter - bounded random walk
    Decorrelated,
}

impl Default for JitterStrategy {
    fn default() -> Self {
        JitterStrategy::Equal
    }
}

/// Backoff strategy configuration
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// Initial delay for first retry
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for exponential backoff
    pub multiplier: f64,
    /// Jitter strategy to use
    pub jitter_strategy: JitterStrategy,
    /// Maximum number of retry attempts
    pub max_attempts: u32,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        BackoffConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            jitter_strategy: JitterStrategy::Equal,
            max_attempts: 3,
        }
    }
}

/// Configuration for Redis connection creation
#[derive(Debug, Clone)]
pub struct RedisConnectionConfig {
    pub host_port: String,
    pub database: i64,
    pub backoff_config: BackoffConfig,
    // Deprecated fields for backward compatibility
    pub retry_attempts: Option<u32>,
    pub retry_delay: Option<Duration>,
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

        // Parse backoff configuration from options
        let mut backoff_config = BackoffConfig::default();
        
        if let Some(initial_delay_ms) = opts.get("retry_initial_delay_ms") {
            if let Ok(delay_ms) = initial_delay_ms.parse::<u64>() {
                backoff_config.initial_delay = Duration::from_millis(delay_ms);
            }
        }
        
        if let Some(max_delay_ms) = opts.get("retry_max_delay_ms") {
            if let Ok(delay_ms) = max_delay_ms.parse::<u64>() {
                backoff_config.max_delay = Duration::from_millis(delay_ms);
            }
        }
        
        if let Some(multiplier_str) = opts.get("retry_multiplier") {
            if let Ok(multiplier) = multiplier_str.parse::<f64>() {
                backoff_config.multiplier = multiplier.max(1.0);
            }
        }
        
        if let Some(max_attempts_str) = opts.get("retry_max_attempts") {
            if let Ok(max_attempts) = max_attempts_str.parse::<u32>() {
                backoff_config.max_attempts = max_attempts.max(1);
            }
        }
        
        if let Some(jitter_str) = opts.get("retry_jitter_strategy") {
            backoff_config.jitter_strategy = match jitter_str.to_lowercase().as_str() {
                "none" => JitterStrategy::None,
                "full" => JitterStrategy::Full,
                "equal" => JitterStrategy::Equal,
                "decorrelated" => JitterStrategy::Decorrelated,
                _ => JitterStrategy::Equal, // Default fallback
            };
        }

        // Handle legacy retry parameters for backward compatibility
        let retry_attempts = opts.get("retry_attempts")
            .and_then(|s| s.parse::<u32>().ok());
        let retry_delay = opts.get("retry_delay_ms")
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_millis);

        // Apply legacy parameters if they override the new config
        if let Some(attempts) = retry_attempts {
            backoff_config.max_attempts = attempts;
        }
        if let Some(delay) = retry_delay {
            backoff_config.initial_delay = delay;
        }

        let config = RedisConnectionConfig {
            host_port,
            database,
            backoff_config,
            retry_attempts,
            retry_delay,
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

                // Add redis:// prefix if not present and format with database
                let url = if trimmed.starts_with("redis://") {
                    format!("{}/{}", trimmed, self.database)
                } else {
                    format!("redis://{}/{}", trimmed, self.database)
                };

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

        let url = if self.host_port.starts_with("redis://") {
            format!("{}/{}", self.host_port, self.database)
        } else {
            format!("redis://{}/{}", self.host_port, self.database)
        };

        Ok(url)
    }
}

/// Jitter backoff calculator for implementing various retry strategies
pub struct JitterBackoff {
    config: BackoffConfig,
    current_attempt: u32,
    last_delay: Duration,
    rng: rand::rngs::ThreadRng,
}

impl JitterBackoff {
    /// Create a new jitter backoff calculator
    pub fn new(config: BackoffConfig) -> Self {
        JitterBackoff {
            config,
            current_attempt: 0,
            last_delay: Duration::from_millis(0),
            rng: rand::thread_rng(),
        }
    }

    /// Calculate the next delay with jitter
    pub fn next_delay(&mut self) -> Option<Duration> {
        if self.current_attempt >= self.config.max_attempts {
            return None;
        }

        self.current_attempt += 1;

        if self.current_attempt == 1 {
            // First retry uses initial delay
            let delay = self.apply_jitter(self.config.initial_delay);
            self.last_delay = delay;
            return Some(delay);
        }

        // Calculate exponential backoff
        let base_delay = std::cmp::min(
            Duration::from_millis(
                (self.config.initial_delay.as_millis() as f64 
                    * self.config.multiplier.powi((self.current_attempt - 1) as i32)) as u64
            ),
            self.config.max_delay,
        );

        let delay = self.apply_jitter(base_delay);
        self.last_delay = delay;
        Some(delay)
    }

    /// Apply jitter strategy to the base delay
    fn apply_jitter(&mut self, base_delay: Duration) -> Duration {
        match self.config.jitter_strategy {
            JitterStrategy::None => base_delay,
            JitterStrategy::Full => {
                // Random delay between 0 and base_delay
                let jitter_ms = self.rng.gen_range(0..=base_delay.as_millis() as u64);
                Duration::from_millis(jitter_ms)
            }
            JitterStrategy::Equal => {
                // Half fixed delay + half random delay
                let half_delay = base_delay.as_millis() as u64 / 2;
                let jitter_ms = self.rng.gen_range(0..=half_delay);
                Duration::from_millis(half_delay + jitter_ms)
            }
            JitterStrategy::Decorrelated => {
                // Bounded random walk with 3 * last_delay as upper bound
                let upper_bound = std::cmp::min(
                    base_delay.as_millis() as u64,
                    3 * self.last_delay.as_millis() as u64,
                );
                let lower_bound = base_delay.as_millis() as u64 / 3;
                
                if upper_bound <= lower_bound {
                    return base_delay;
                }
                
                let jitter_ms = self.rng.gen_range(lower_bound..=upper_bound);
                Duration::from_millis(jitter_ms)
            }
        }
    }

    /// Get the current attempt number
    pub fn current_attempt(&self) -> u32 {
        self.current_attempt
    }

    /// Check if more retries are available
    pub fn has_more_attempts(&self) -> bool {
        self.current_attempt < self.config.max_attempts
    }

    /// Reset the backoff calculator
    pub fn reset(&mut self) {
        self.current_attempt = 0;
        self.last_delay = Duration::from_millis(0);
    }
}

/// Redis connection factory for creating properly configured connections
pub struct RedisConnectionFactory;

impl RedisConnectionFactory {
    /// Create a Redis client based on configuration
    fn create_client(config: &RedisConnectionConfig) -> ConnectionFactoryResult<Client> {
        let url = config.get_single_node_url()?;
        log!("Creating single Redis node connection: {}", url);
        let client = Client::open(url)?;
        Ok(client)
    }

    /// Create a Redis cluster client based on configuration
    fn create_cluster_client(
        config: &RedisConnectionConfig,
    ) -> ConnectionFactoryResult<ClusterClient> {
        let nodes = config.parse_cluster_nodes()?;
        log!("Creating Redis cluster connection with nodes: {:?}", nodes);
        let cluster_client = ClusterClient::new(nodes)?;
        Ok(cluster_client)
    }

    /// Create a connection with jitter retry logic
    pub fn create_connection_with_retry(
        config: &RedisConnectionConfig,
    ) -> ConnectionFactoryResult<RedisConnectionType> {
        let mut backoff = JitterBackoff::new(config.backoff_config.clone());
        
        // First attempt without delay
        match Self::create_connection_internal(config) {
            Ok(connection) => {
                log!("Successfully created Redis connection on first attempt");
                return Ok(connection);
            }
            Err(e) => {
                log!("Initial connection attempt failed: {}", e);
            }
        }

        // Retry with jitter backoff
        while let Some(delay) = backoff.next_delay() {
            log!(
                "Connection attempt {} failed, retrying in {:?}",
                backoff.current_attempt(),
                delay
            );
            
            std::thread::sleep(delay);
            
            match Self::create_connection_internal(config) {
                Ok(connection) => {
                    log!(
                        "Successfully created Redis connection on attempt {}",
                        backoff.current_attempt() + 1 // +1 because we count initial attempt
                    );
                    return Ok(connection);
                }
                Err(e) if backoff.has_more_attempts() => {
                    log!(
                        "Connection attempt {} failed: {}",
                        backoff.current_attempt() + 1,
                        e
                    );
                    continue;
                }
                Err(e) => {
                    return Err(ConnectionFactoryError::ConnectionFailed(format!(
                        "Failed to connect after {} attempts: {}",
                        backoff.current_attempt() + 1,
                        e
                    )));
                }
            }
        }

        // Fallback to legacy retry logic for backward compatibility
        Self::create_connection_with_legacy_retry(config)
    }

    /// retry method for backward compatibility
    fn create_connection_with_legacy_retry(
        config: &RedisConnectionConfig,
    ) -> ConnectionFactoryResult<RedisConnectionType> {
        let retry_attempts = config.retry_attempts.unwrap_or(3);
        let retry_delay = config.retry_delay.unwrap_or(Duration::from_millis(100));

        for attempt in 1..=retry_attempts {
            match Self::create_connection_internal(config) {
                Ok(connection) => {
                    log!(
                        "Successfully created Redis connection on attempt {attempt}"
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

    /// Internal connection creation logic
    fn create_connection_internal(
        config: &RedisConnectionConfig,
    ) -> ConnectionFactoryResult<RedisConnectionType> {
        if config.is_cluster_mode() {
            let cluster_client = Self::create_cluster_client(config)?;
            let cluster_connection = cluster_client
                .get_connection()
                .map_err(|e| ConnectionFactoryError::ConnectionFailed(e.to_string()))?;

            Ok(RedisConnectionType::Cluster(cluster_connection))
        } else {
            let client = Self::create_client(config)?;
            let connection = client
                .get_connection()
                .map_err(|e| ConnectionFactoryError::ConnectionFailed(e.to_string()))?;

            Ok(RedisConnectionType::Single(connection))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_connection_config_from_options() {
        let mut opts = HashMap::new();
        opts.insert("host_port".to_string(), "127.0.0.1:6379".to_string());
        opts.insert("database".to_string(), "1".to_string());

        let config = RedisConnectionConfig::from_options(&opts).unwrap();
        assert_eq!(config.host_port, "127.0.0.1:6379");
        assert_eq!(config.database, 1);
        assert!(!config.is_cluster_mode());
    }

    #[test]
    fn test_cluster_mode_detection() {
        let mut opts = HashMap::new();
        opts.insert(
            "host_port".to_string(),
            "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002".to_string(),
        );
        opts.insert("database".to_string(), "0".to_string());

        let config = RedisConnectionConfig::from_options(&opts).unwrap();
        assert!(config.is_cluster_mode());
    }

    #[test]
    fn test_cluster_nodes_parsing() {
        let mut opts = HashMap::new();
        opts.insert(
            "host_port".to_string(),
            "127.0.0.1:7000,127.0.0.1:7001".to_string(),
        );
        opts.insert("database".to_string(), "0".to_string());

        let config = RedisConnectionConfig::from_options(&opts).unwrap();
        let nodes = config.parse_cluster_nodes().unwrap();

        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0], "redis://127.0.0.1:7000/0");
        assert_eq!(nodes[1], "redis://127.0.0.1:7001/0");
    }

    #[test]
    fn test_single_node_url() {
        let mut opts = HashMap::new();
        opts.insert("host_port".to_string(), "127.0.0.1:6379".to_string());
        opts.insert("database".to_string(), "2".to_string());

        let config = RedisConnectionConfig::from_options(&opts).unwrap();
        let url = config.get_single_node_url().unwrap();

        assert_eq!(url, "redis://127.0.0.1:6379/2");
    }

    #[test]
    fn test_validation_missing_host_port() {
        let opts = HashMap::new();
        let result = RedisConnectionConfig::from_options(&opts);

        assert!(result.is_err());
        match result.unwrap_err() {
            ConnectionFactoryError::MissingConfiguration(msg) => {
                assert_eq!(msg, "host_port");
            }
            _ => panic!("Expected MissingConfiguration error"),
        }
    }

    #[test]
    fn test_validation_invalid_database() {
        let mut opts = HashMap::new();
        opts.insert("host_port".to_string(), "127.0.0.1:6379".to_string());
        opts.insert("database".to_string(), "16".to_string());

        let result = RedisConnectionConfig::from_options(&opts);

        assert!(result.is_err());
        match result.unwrap_err() {
            ConnectionFactoryError::InvalidDatabase(db) => {
                assert_eq!(db, 16);
            }
            _ => panic!("Expected InvalidDatabase error"),
        }
    }

    #[test]
    fn test_url_with_prefix() {
        let mut opts = HashMap::new();
        opts.insert(
            "host_port".to_string(),
            "redis://127.0.0.1:6379".to_string(),
        );
        opts.insert("database".to_string(), "0".to_string());

        let config = RedisConnectionConfig::from_options(&opts).unwrap();
        let url = config.get_single_node_url().unwrap();

        assert_eq!(url, "redis://127.0.0.1:6379/0");
    }

    #[test]
    fn test_jitter_backoff_config_parsing() {
        let mut opts = HashMap::new();
        opts.insert("host_port".to_string(), "127.0.0.1:6379".to_string());
        opts.insert("database".to_string(), "0".to_string());
        opts.insert("retry_initial_delay_ms".to_string(), "200".to_string());
        opts.insert("retry_max_delay_ms".to_string(), "60000".to_string());
        opts.insert("retry_multiplier".to_string(), "1.5".to_string());
        opts.insert("retry_max_attempts".to_string(), "5".to_string());
        opts.insert("retry_jitter_strategy".to_string(), "full".to_string());

        let config = RedisConnectionConfig::from_options(&opts).unwrap();
        
        assert_eq!(config.backoff_config.initial_delay, Duration::from_millis(200));
        assert_eq!(config.backoff_config.max_delay, Duration::from_millis(60000));
        assert_eq!(config.backoff_config.multiplier, 1.5);
        assert_eq!(config.backoff_config.max_attempts, 5);
        assert!(matches!(config.backoff_config.jitter_strategy, JitterStrategy::Full));
    }

    #[test]
    fn test_jitter_strategy_parsing() {
        let test_cases = vec![
            ("none", JitterStrategy::None),
            ("full", JitterStrategy::Full),
            ("equal", JitterStrategy::Equal),
            ("decorrelated", JitterStrategy::Decorrelated),
            ("invalid", JitterStrategy::Equal), // Should fallback to Equal
        ];

        for (input, expected) in test_cases {
            let mut opts = HashMap::new();
            opts.insert("host_port".to_string(), "127.0.0.1:6379".to_string());
            opts.insert("retry_jitter_strategy".to_string(), input.to_string());

            let config = RedisConnectionConfig::from_options(&opts).unwrap();
            assert!(matches!(config.backoff_config.jitter_strategy, expected));
        }
    }

    #[test]
    fn test_legacy_retry_parameter_compatibility() {
        let mut opts = HashMap::new();
        opts.insert("host_port".to_string(), "127.0.0.1:6379".to_string());
        opts.insert("retry_attempts".to_string(), "7".to_string());
        opts.insert("retry_delay_ms".to_string(), "500".to_string());

        let config = RedisConnectionConfig::from_options(&opts).unwrap();
        
        // Legacy parameters should override defaults
        assert_eq!(config.backoff_config.max_attempts, 7);
        assert_eq!(config.backoff_config.initial_delay, Duration::from_millis(500));
        assert_eq!(config.retry_attempts, Some(7));
        assert_eq!(config.retry_delay, Some(Duration::from_millis(500)));
    }

    #[test]
    fn test_jitter_backoff_no_jitter() {
        let config = BackoffConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
            jitter_strategy: JitterStrategy::None,
            max_attempts: 3,
        };

        let mut backoff = JitterBackoff::new(config);
        
        // Test delay progression without jitter
        assert_eq!(backoff.next_delay(), Some(Duration::from_millis(100))); // First retry
        assert_eq!(backoff.current_attempt(), 1);
        
        assert_eq!(backoff.next_delay(), Some(Duration::from_millis(200))); // Second retry (100 * 2^1)
        assert_eq!(backoff.current_attempt(), 2);
        
        assert_eq!(backoff.next_delay(), Some(Duration::from_millis(400))); // Third retry (100 * 2^2)
        assert_eq!(backoff.current_attempt(), 3);
        
        assert_eq!(backoff.next_delay(), None); // No more attempts
        assert!(!backoff.has_more_attempts());
    }

    #[test]
    fn test_jitter_backoff_max_delay_capping() {
        let config = BackoffConfig {
            initial_delay: Duration::from_millis(1000),
            max_delay: Duration::from_millis(2000), // Cap at 2 seconds
            multiplier: 3.0,
            jitter_strategy: JitterStrategy::None,
            max_attempts: 4,
        };

        let mut backoff = JitterBackoff::new(config);
        
        assert_eq!(backoff.next_delay(), Some(Duration::from_millis(1000))); // First retry
        assert_eq!(backoff.next_delay(), Some(Duration::from_millis(2000))); // Capped at max_delay
        assert_eq!(backoff.next_delay(), Some(Duration::from_millis(2000))); // Still capped
        assert_eq!(backoff.next_delay(), Some(Duration::from_millis(2000))); // Still capped
        assert_eq!(backoff.next_delay(), None); // No more attempts
    }

    #[test]
    fn test_jitter_backoff_full_jitter_bounds() {
        let config = BackoffConfig {
            initial_delay: Duration::from_millis(1000),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
            jitter_strategy: JitterStrategy::Full,
            max_attempts: 3,
        };

        let mut backoff = JitterBackoff::new(config);
        
        // Test that full jitter is within bounds
        for _ in 0..3 {
            if let Some(delay) = backoff.next_delay() {
                assert!(delay <= Duration::from_millis(1000 * 2_u64.pow(backoff.current_attempt() - 1)));
                assert!(delay >= Duration::from_millis(0));
            }
        }
    }

    #[test]
    fn test_jitter_backoff_equal_jitter_bounds() {
        let config = BackoffConfig {
            initial_delay: Duration::from_millis(1000),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
            jitter_strategy: JitterStrategy::Equal,
            max_attempts: 3,
        };

        let mut backoff = JitterBackoff::new(config);
        
        // Test that equal jitter is within reasonable bounds
        if let Some(delay) = backoff.next_delay() {
            // Equal jitter should be at least half the base delay
            assert!(delay >= Duration::from_millis(500));
            assert!(delay <= Duration::from_millis(1000));
        }
    }

    #[test]
    fn test_jitter_backoff_reset() {
        let config = BackoffConfig::default();
        let mut backoff = JitterBackoff::new(config);
        
        // Use some attempts
        backoff.next_delay();
        backoff.next_delay();
        assert_eq!(backoff.current_attempt(), 2);
        
        // Reset should bring us back to initial state
        backoff.reset();
        assert_eq!(backoff.current_attempt(), 0);
        assert!(backoff.has_more_attempts());
    }

    #[test]
    fn test_default_backoff_config() {
        let config = BackoffConfig::default();
        
        assert_eq!(config.initial_delay, Duration::from_millis(100));
        assert_eq!(config.max_delay, Duration::from_secs(30));
        assert_eq!(config.multiplier, 2.0);
        assert_eq!(config.max_attempts, 3);
        assert!(matches!(config.jitter_strategy, JitterStrategy::Equal));
    }

    #[test]
    fn test_config_validation_with_jitter_options() {
        let mut opts = HashMap::new();
        opts.insert("host_port".to_string(), "127.0.0.1:6379".to_string());
        opts.insert("database".to_string(), "0".to_string());
        opts.insert("retry_multiplier".to_string(), "0.5".to_string()); // Should be clamped to 1.0
        opts.insert("retry_max_attempts".to_string(), "0".to_string()); // Should be clamped to 1

        let config = RedisConnectionConfig::from_options(&opts).unwrap();
        
        assert_eq!(config.backoff_config.multiplier, 1.0); // Clamped
        assert_eq!(config.backoff_config.max_attempts, 1); // Clamped
    }
}
