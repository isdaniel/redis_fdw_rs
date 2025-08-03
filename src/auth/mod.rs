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
}
