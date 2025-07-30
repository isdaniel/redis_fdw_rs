/// Comprehensive integration tests for Redis FDW table types
///
/// This module provides integration tests for all Redis table types (String, Hash, List, Set, ZSet)
/// focusing on the table logic, data structures, and operations without requiring external Redis.
/// These tests verify the table implementations, data handling, and type conversions.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::{auth::RedisAuthConfig, core::connection_factory::{ConnectionFactoryError, RedisConnectionConfig}};
    use std::collections::HashMap;

    #[test]
    fn test_auth_config_creation() {
        let mut opts = HashMap::new();
        opts.insert("password".to_string(), "secret123".to_string());
        
        let config = RedisAuthConfig::from_user_mapping_options(&opts);
        assert!(config.is_auth_required());
        assert_eq!(config.password, Some("secret123".to_string()));
        assert_eq!(config.username, None);
    }

    #[test]
    fn test_auth_config_with_username() {
        let mut opts = HashMap::new();
        opts.insert("password".to_string(), "secret123".to_string());
        opts.insert("username".to_string(), "redis_user".to_string());
        
        let config = RedisAuthConfig::from_user_mapping_options(&opts);
        assert!(config.is_auth_required());
        assert_eq!(config.password, Some("secret123".to_string()));
        assert_eq!(config.username, Some("redis_user".to_string()));
    }

    #[test]
    fn test_auth_url_component() {
        let config = RedisAuthConfig {
            password: Some("secret123".to_string()),
            username: None,
        };
        assert_eq!(config.get_auth_url_component(), ":secret123@");

        let config = RedisAuthConfig {
            password: Some("secret123".to_string()),
            username: Some("redis_user".to_string()),
        };
        assert_eq!(config.get_auth_url_component(), "redis_user:secret123@");

        let config = RedisAuthConfig {
            password: None,
            username: None,
        };
        assert_eq!(config.get_auth_url_component(), "");
    }

    #[test]
    fn test_apply_to_url() {
        let config = RedisAuthConfig {
            password: Some("secret123".to_string()),
            username: None,
        };

        // Test with redis:// URL
        let url = "redis://127.0.0.1:6379/0";
        let result = config.apply_to_url(url);
        assert_eq!(result, "redis://:secret123@127.0.0.1:6379/0");

        // Test with plain host:port
        let url = "127.0.0.1:6379";
        let result = config.apply_to_url(url);
        assert_eq!(result, "redis://:secret123@127.0.0.1:6379");
    }

    #[test]
    fn test_apply_to_url_with_username() {
        let config = RedisAuthConfig {
            password: Some("secret123".to_string()),
            username: Some("redis_user".to_string()),
        };

        let url = "redis://127.0.0.1:6379/0";
        let result = config.apply_to_url(url);
        assert_eq!(result, "redis://redis_user:secret123@127.0.0.1:6379/0");
    }

    #[test]
    fn test_no_auth_required() {
        let config = RedisAuthConfig::default();
        
        let url = "redis://127.0.0.1:6379/0";
        let result = config.apply_to_url(url);
        assert_eq!(result, url);
    }

    #[test]
    fn test_replace_existing_auth() {
        let config = RedisAuthConfig {
            password: Some("newsecret".to_string()),
            username: Some("newuser".to_string()),
        };

        let url = "redis://olduser:oldpass@127.0.0.1:6379/0";
        let result = config.apply_to_url(url);
        assert_eq!(result, "redis://newuser:newsecret@127.0.0.1:6379/0");
    }

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
    fn test_authentication_single_node() {

        let mut opts = HashMap::new();
        opts.insert("host_port".to_string(), "127.0.0.1:6379".to_string());
        opts.insert("database".to_string(), "0".to_string());
        opts.insert("password".to_string(), "secret123".to_string());

        let config = RedisConnectionConfig::from_options(&opts).unwrap();
        let url = config.get_single_node_url().unwrap();

        assert_eq!(url, "redis://:secret123@127.0.0.1:6379/0");
    }

    #[test]
    fn test_authentication_with_username() {
        
        let mut opts = HashMap::new();
        opts.insert("host_port".to_string(), "127.0.0.1:6379".to_string());
        opts.insert("database".to_string(), "0".to_string());
        opts.insert("username".to_string(), "redis_user".to_string());
        opts.insert("password".to_string(), "secret123".to_string());

        let config = RedisConnectionConfig::from_options(&opts).unwrap();
        let url = config.get_single_node_url().unwrap();

        assert_eq!(url, "redis://redis_user:secret123@127.0.0.1:6379/0");
    }

    #[test]
    fn test_authentication_cluster() {
        
        let mut opts = HashMap::new();
        opts.insert(
            "host_port".to_string(),
            "127.0.0.1:7000,127.0.0.1:7001".to_string(),
        );
        opts.insert("database".to_string(), "0".to_string());
        opts.insert("password".to_string(), "secret123".to_string());

        let config = RedisConnectionConfig::from_options(&opts).unwrap();
        let nodes = config.parse_cluster_nodes().unwrap();

        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0], "redis://:secret123@127.0.0.1:7000/0");
        assert_eq!(nodes[1], "redis://:secret123@127.0.0.1:7001/0");
    }
}
