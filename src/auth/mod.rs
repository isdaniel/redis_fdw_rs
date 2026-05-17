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

        // Handle different URL formats
        if url.starts_with("redis://") {
            // Remove existing auth if present
            let url_without_auth = if let Some(at_pos) = url.find('@') {
                // Check if there's a scheme before @ to distinguish auth from domain
                if url[7..at_pos].contains(':') {
                    format!("redis://{}", &url[at_pos + 1..])
                } else {
                    url.to_string()
                }
            } else {
                url.to_string()
            };

            // Insert auth component
            format!("redis://{}{}", auth_component, &url_without_auth[8..])
        } else {
            // For non-redis:// URLs, prepend redis:// with auth
            format!("redis://{}{}", auth_component, url)
        }
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
}
