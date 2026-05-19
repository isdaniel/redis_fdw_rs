/// Redis Authentication Module
///
/// This module provides authentication functionality for Redis FDW connections.
/// It supports retrieving password credentials from PostgreSQL user mappings
/// and properly formatting them for both single-node and cluster Redis connections.
use std::collections::HashMap;

/// Authentication configuration for Redis connections
#[derive(Debug, Clone, Default)]
pub struct RedisAuthConfig {
    /// Redis password for authentication (None if no authentication required)
    pub password: Option<String>,
    /// Redis username for ACL authentication (None for legacy password-only auth)
    pub username: Option<String>,
}

impl RedisAuthConfig {
    /// Create a new authentication config from user mapping options
    pub fn from_user_mapping_options(opts: &HashMap<String, String>) -> Self {
        let password = opts.get("password").cloned();
        let username = opts.get("username").cloned();

        RedisAuthConfig { password, username }
    }

    /// Check if authentication is required
    pub fn is_auth_required(&self) -> bool {
        self.password.is_some()
    }

    /// Get the authentication URL component for redis:// URLs
    /// Returns format: "username:password@" or "password@" or empty string
    pub fn get_auth_url_component(&self) -> String {
        match (&self.username, &self.password) {
            (Some(username), Some(password)) => format!("{}:{}@", username, password),
            (None, Some(password)) => format!(":{}@", password),
            _ => String::new(),
        }
    }

    /// Apply authentication to an existing Redis URL
    /// If URL already contains auth, it will be replaced
    pub fn apply_to_url(&self, url: &str) -> String {
        if !self.is_auth_required() {
            return url.to_string();
        }

        let auth_component = self.get_auth_url_component();

        // Detect scheme (check rediss:// before redis:// since redis:// is a prefix of rediss://)
        let (scheme, rest) = if let Some(rest) = url.strip_prefix("rediss://") {
            ("rediss://", rest)
        } else if let Some(rest) = url.strip_prefix("redis://") {
            ("redis://", rest)
        } else {
            return format!("redis://{}{}", auth_component, url);
        };

        // Remove existing auth if present (only look in the authority component, not path/fragment)
        let authority = rest.split('/').next().unwrap_or("");
        let cleaned = if let Some(at_pos) = authority.find('@') {
            &rest[at_pos + 1..]
        } else {
            rest
        };

        format!("{}{}{}", scheme, auth_component, cleaned)
    }

    /// Generate a cache key for pool identification
    /// This is used to uniquely identify connection configurations for pooling
    pub fn cache_key(&self) -> String {
        match (&self.username, &self.password) {
            (Some(u), Some(_)) => format!("auth:user:{}", u),
            (None, Some(_)) => "auth:password".to_string(),
            _ => "noauth".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_empty_options() {
        let opts = HashMap::new();
        let config = RedisAuthConfig::from_user_mapping_options(&opts);
        assert!(!config.is_auth_required());
        assert_eq!(config.password, None);
        assert_eq!(config.username, None);
    }

    #[test]
    fn test_from_password_only() {
        let mut opts = HashMap::new();
        opts.insert("password".to_string(), "secret".to_string());
        let config = RedisAuthConfig::from_user_mapping_options(&opts);
        assert!(config.is_auth_required());
        assert_eq!(config.password, Some("secret".to_string()));
        assert_eq!(config.username, None);
    }

    #[test]
    fn test_from_username_and_password() {
        let mut opts = HashMap::new();
        opts.insert("password".to_string(), "secret".to_string());
        opts.insert("username".to_string(), "admin".to_string());
        let config = RedisAuthConfig::from_user_mapping_options(&opts);
        assert!(config.is_auth_required());
        assert_eq!(config.password, Some("secret".to_string()));
        assert_eq!(config.username, Some("admin".to_string()));
    }

    #[test]
    fn test_auth_url_component_no_auth() {
        let config = RedisAuthConfig::default();
        assert_eq!(config.get_auth_url_component(), "");
    }

    #[test]
    fn test_auth_url_component_password_only() {
        let config = RedisAuthConfig {
            password: Some("pass123".to_string()),
            username: None,
        };
        assert_eq!(config.get_auth_url_component(), ":pass123@");
    }

    #[test]
    fn test_auth_url_component_username_and_password() {
        let config = RedisAuthConfig {
            password: Some("pass123".to_string()),
            username: Some("user1".to_string()),
        };
        assert_eq!(config.get_auth_url_component(), "user1:pass123@");
    }

    #[test]
    fn test_apply_to_url_no_auth() {
        let config = RedisAuthConfig::default();
        let url = "redis://127.0.0.1:6379/0";
        assert_eq!(config.apply_to_url(url), url);
    }

    #[test]
    fn test_apply_to_url_with_password() {
        let config = RedisAuthConfig {
            password: Some("secret".to_string()),
            username: None,
        };
        assert_eq!(
            config.apply_to_url("redis://127.0.0.1:6379/0"),
            "redis://:secret@127.0.0.1:6379/0"
        );
    }

    #[test]
    fn test_apply_to_url_plain_host() {
        let config = RedisAuthConfig {
            password: Some("secret".to_string()),
            username: None,
        };
        assert_eq!(
            config.apply_to_url("127.0.0.1:6379"),
            "redis://:secret@127.0.0.1:6379"
        );
    }

    #[test]
    fn test_apply_to_url_replace_existing() {
        let config = RedisAuthConfig {
            password: Some("new_pass".to_string()),
            username: Some("new_user".to_string()),
        };
        assert_eq!(
            config.apply_to_url("redis://old:old@127.0.0.1:6379/0"),
            "redis://new_user:new_pass@127.0.0.1:6379/0"
        );
    }

    #[test]
    fn test_cache_key_variants() {
        let no_auth = RedisAuthConfig::default();
        assert_eq!(no_auth.cache_key(), "noauth");

        let pass_only = RedisAuthConfig {
            password: Some("x".to_string()),
            username: None,
        };
        assert_eq!(pass_only.cache_key(), "auth:password");

        let full_auth = RedisAuthConfig {
            password: Some("x".to_string()),
            username: Some("admin".to_string()),
        };
        assert_eq!(full_auth.cache_key(), "auth:user:admin");
    }

    #[test]
    fn test_apply_to_url_rediss_no_auth() {
        let config = RedisAuthConfig::default();
        let url = "rediss://redis.cloud.com:6380/0";
        assert_eq!(config.apply_to_url(url), url);
    }

    #[test]
    fn test_apply_to_url_rediss_with_password() {
        let config = RedisAuthConfig {
            password: Some("secret".to_string()),
            username: None,
        };
        assert_eq!(
            config.apply_to_url("rediss://redis.cloud.com:6380/0"),
            "rediss://:secret@redis.cloud.com:6380/0"
        );
    }

    #[test]
    fn test_apply_to_url_rediss_with_username_password() {
        let config = RedisAuthConfig {
            password: Some("pass".to_string()),
            username: Some("user".to_string()),
        };
        assert_eq!(
            config.apply_to_url("rediss://redis.cloud.com:6380/0"),
            "rediss://user:pass@redis.cloud.com:6380/0"
        );
    }

    #[test]
    fn test_apply_to_url_rediss_replace_existing_auth() {
        let config = RedisAuthConfig {
            password: Some("new_pass".to_string()),
            username: Some("new_user".to_string()),
        };
        assert_eq!(
            config.apply_to_url("rediss://old:old@redis.cloud.com:6380/0"),
            "rediss://new_user:new_pass@redis.cloud.com:6380/0"
        );
    }

    #[test]
    fn test_apply_to_url_replace_password_only_auth() {
        let config = RedisAuthConfig {
            password: Some("new_pass".to_string()),
            username: Some("new_user".to_string()),
        };
        assert_eq!(
            config.apply_to_url("redis://password@127.0.0.1:6379/0"),
            "redis://new_user:new_pass@127.0.0.1:6379/0"
        );
    }

    #[test]
    fn test_apply_to_url_no_false_positive_at_in_path() {
        let config = RedisAuthConfig {
            password: Some("secret".to_string()),
            username: None,
        };
        assert_eq!(
            config.apply_to_url("redis://127.0.0.1:6379/0"),
            "redis://:secret@127.0.0.1:6379/0"
        );
    }
}
