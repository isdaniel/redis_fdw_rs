/// Redis SCAN operations and pattern matching utilities
/// This module provides comprehensive SCAN support for all Redis data types
/// with LIKE pattern matching capabilities for WHERE clause optimization.
use crate::query::{
    limit::LimitOffsetInfo,
    pushdown_types::{ComparisonOperator, PushableCondition},
};
use redis::{ConnectionLike, RedisError, RedisResult};

/// Redis SCAN operation types
#[derive(Debug, Clone, PartialEq)]
pub enum ScanType {
    /// Database key scan (SCAN)
    KeyScan,
    /// Hash field scan (HSCAN)
    HashScan,
    /// Set member scan (SSCAN)
    SetScan,
    /// Sorted set member scan (ZSCAN)
    ZSetScan,
}

/// Pattern matching utilities for Redis glob patterns
#[derive(Debug, Clone)]
pub struct PatternMatcher {
    pattern: String,
    is_wildcard: bool,
}

impl PatternMatcher {
    /// Create a new pattern matcher from a LIKE expression
    pub fn from_like_pattern(like_pattern: &str) -> Self {
        // Convert SQL LIKE pattern to Redis glob pattern
        let redis_pattern = Self::convert_like_to_glob(like_pattern);
        let is_wildcard = redis_pattern.contains('*')
            || redis_pattern.contains('?')
            || redis_pattern.contains('[');

        Self {
            pattern: redis_pattern,
            is_wildcard,
        }
    }

    /// Convert SQL LIKE pattern to Redis glob pattern
    fn convert_like_to_glob(like_pattern: &str) -> String {
        like_pattern
            .replace("%", "*") // SQL % becomes Redis *
            .replace("_", "?") // SQL _ becomes Redis ?
    }

    /// Get the Redis-compatible glob pattern
    #[inline]
    pub fn get_pattern(&self) -> &str {
        &self.pattern
    }

    /// Check if this pattern requires SCAN with MATCH
    #[inline]
    pub fn requires_scan(&self) -> bool {
        self.is_wildcard
    }

    /// Check if a string matches this pattern (for client-side filtering)
    pub fn matches(&self, text: &str) -> bool {
        if !self.is_wildcard {
            return text == self.pattern;
        }

        // Simple glob matching implementation
        glob_match(&self.pattern, text)
    }
}

/// Configuration for different scan command types
#[derive(Debug)]
struct ScanConfig {
    command_name: &'static str,
    requires_key: bool,
    limit_multiply_size: usize,
    default_error_msg: &'static str,
}

impl ScanConfig {
    fn for_scan_type(scan_type: &ScanType) -> Self {
        match scan_type {
            ScanType::KeyScan => ScanConfig {
                command_name: "SCAN",
                requires_key: false,
                limit_multiply_size: 2,
                default_error_msg: "Key scan error",
            },
            ScanType::HashScan => ScanConfig {
                command_name: "HSCAN",
                requires_key: true,
                limit_multiply_size: 2,
                default_error_msg: "Hash key is required for HSCAN",
            },
            ScanType::SetScan => ScanConfig {
                command_name: "SSCAN",
                requires_key: true,
                limit_multiply_size: 1,
                default_error_msg: "Set key is required for SSCAN",
            },
            ScanType::ZSetScan => ScanConfig {
                command_name: "ZSCAN",
                requires_key: true,
                limit_multiply_size: 1,
                default_error_msg: "ZSet key is required for ZSCAN",
            },
        }
    }
}

/// SCAN operation builder for different Redis data types
#[derive(Debug)]
pub struct RedisScanBuilder {
    scan_type: ScanType,
    key: Option<String>,
    pattern: Option<String>,
    limit: Option<LimitOffsetInfo>,
}

impl RedisScanBuilder {
    const SCAN_DEFAULT_COUNT: usize = 5000;
    /// Create a new SCAN builder (default to key scan)
    pub fn new() -> Self {
        Self {
            scan_type: ScanType::KeyScan,
            key: None,
            pattern: None,
            limit: None,
        }
    }

    /// Create a new SCAN builder for database keys
    pub fn new_key_scan() -> Self {
        Self {
            scan_type: ScanType::KeyScan,
            key: None,
            pattern: None,
            limit: None,
        }
    }

    /// Create a new HSCAN builder for hash fields
    pub fn new_hash_scan(key: &str) -> Self {
        Self {
            scan_type: ScanType::HashScan,
            key: Some(key.to_string()),
            pattern: None,
            limit: None,
        }
    }

    /// Create a new SSCAN builder for set members
    pub fn new_set_scan(key: &str) -> Self {
        Self {
            scan_type: ScanType::SetScan,
            key: Some(key.to_string()),
            pattern: None,
            limit: None,
        }
    }

    /// Create a new ZSCAN builder for sorted set members
    pub fn new_zset_scan(key: &str) -> Self {
        Self {
            scan_type: ScanType::ZSetScan,
            key: Some(key.to_string()),
            pattern: None,
            limit: None,
        }
    }

    /// Set the pattern for MATCH filtering
    pub fn with_pattern(mut self, pattern: &str) -> Self {
        self.pattern = Some(pattern.to_string());
        self
    }

    /// Set the limit hint for the SCAN operation
    pub fn with_limit(mut self, limit: LimitOffsetInfo) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Execute the SCAN operation and return all matching results
    pub fn execute_all<T>(&self, conn: &mut dyn ConnectionLike) -> RedisResult<Vec<T>>
    where
        T: redis::FromRedisValue + std::fmt::Debug,
    {
        let config = ScanConfig::for_scan_type(&self.scan_type);
        self.execute_scan(conn, &config)
    }

    /// Generic scan execution function that handles all scan types
    fn execute_scan<T>(
        &self,
        conn: &mut dyn ConnectionLike,
        config: &ScanConfig,
    ) -> RedisResult<Vec<T>>
    where
        T: redis::FromRedisValue + std::fmt::Debug,
    {
        // Validate key requirement
        if config.requires_key && self.key.is_none() {
            return Err(RedisError::from((
                redis::ErrorKind::TypeError,
                config.default_error_msg,
            )));
        }

        // Calculate limit
        let mut limit: usize = 1000;
        if let Some(limit_offset) = &self.limit {
            let base_limit = limit_offset.limit.unwrap_or(limit);
            limit = base_limit * config.limit_multiply_size;
        }

        let mut collected_results = Vec::with_capacity(limit);
        let mut cursor = 0;

        loop {
            let mut cmd = redis::cmd(config.command_name);

            // Add arguments based on scan type
            if config.requires_key {
                if let Some(key) = &self.key {
                    cmd.arg(key);
                }
            }
            cmd.arg(cursor);

            // Add pattern if specified
            if let Some(pattern) = &self.pattern {
                cmd.arg("MATCH").arg(pattern);
            }

            // Add COUNT for most scan types (ZSCAN has special handling)
            if self.scan_type != ScanType::ZSetScan {
                cmd.arg("COUNT").arg(Self::SCAN_DEFAULT_COUNT);
            }

            let (new_cursor, results): (u64, Vec<T>) = cmd.query(conn)?;

            // Handle limit checking - ZSCAN doesn't apply limit during scanning
            if self.scan_type == ScanType::ZSetScan {
                collected_results.extend(results);
            } else {
                for item in results {
                    if collected_results.len() < limit {
                        collected_results.push(item);
                    } else {
                        return Ok(collected_results);
                    }
                }
            }

            if new_cursor == 0 {
                break;
            }
            cursor = new_cursor;
        }

        Ok(collected_results)
    }
}

/// Extract optimizable conditions for SCAN operations
pub fn extract_scan_conditions(conditions: &[PushableCondition]) -> ScanConditions {
    let mut pattern_conditions = Vec::new();
    let mut exact_conditions = Vec::new();
    let mut pattern_matcher = None;

    for condition in conditions {
        match &condition.operator {
            ComparisonOperator::Like => {
                // Create pattern matcher from the first LIKE condition
                if pattern_matcher.is_none() {
                    pattern_matcher = Some(PatternMatcher::from_like_pattern(&condition.value));
                }
                pattern_conditions.push(condition.clone());
            }
            ComparisonOperator::Equal => {
                exact_conditions.push(condition.clone());
            }
            _ => {
                // Other operators are not optimizable for SCAN
            }
        }
    }

    ScanConditions {
        pattern_conditions,
        exact_conditions,
        pattern_matcher,
    }
}

/// Conditions that can be optimized with SCAN operations
#[derive(Debug, Clone)]
pub struct ScanConditions {
    pub pattern_conditions: Vec<PushableCondition>,
    pub exact_conditions: Vec<PushableCondition>,
    pub pattern_matcher: Option<PatternMatcher>,
}

impl ScanConditions {
    /// Check if we have any optimizable conditions
    pub fn has_optimizable_conditions(&self) -> bool {
        !self.pattern_conditions.is_empty() || !self.exact_conditions.is_empty()
    }

    /// Get the most restrictive pattern for SCAN optimization
    pub fn get_primary_pattern(&self) -> Option<String> {
        // Prefer exact matches first
        if let Some(exact) = self.exact_conditions.first() {
            return Some(exact.value.clone());
        }

        // Then use pattern matches
        if let Some(pattern) = self.pattern_conditions.first() {
            let matcher = PatternMatcher::from_like_pattern(&pattern.value);
            return Some(matcher.get_pattern().to_string());
        }

        None
    }
}

/// Simple glob pattern matching
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();

    fn match_recursive(pattern: &[char], text: &[char], p_idx: usize, t_idx: usize) -> bool {
        if p_idx >= pattern.len() {
            return t_idx >= text.len();
        }

        match pattern[p_idx] {
            '*' => {
                // Try matching zero or more characters
                for i in t_idx..=text.len() {
                    if match_recursive(pattern, text, p_idx + 1, i) {
                        return true;
                    }
                }
                false
            }
            '?' => {
                // Match exactly one character
                if t_idx >= text.len() {
                    false
                } else {
                    match_recursive(pattern, text, p_idx + 1, t_idx + 1)
                }
            }
            c => {
                // Match exact character
                if t_idx >= text.len() || text[t_idx] != c {
                    false
                } else {
                    match_recursive(pattern, text, p_idx + 1, t_idx + 1)
                }
            }
        }
    }

    match_recursive(&pattern_chars, &text_chars, 0, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_matcher_like_conversion() {
        // Test % to * conversion (multiple characters)
        let matcher = PatternMatcher::from_like_pattern("user_%");
        assert_eq!(matcher.get_pattern(), "user?*"); // _ becomes ?, % becomes *
        assert!(matcher.requires_scan());

        // Test _ to ? conversion (single character)
        let matcher = PatternMatcher::from_like_pattern("user_x");
        assert_eq!(matcher.get_pattern(), "user?x"); // _ becomes ?
        assert!(matcher.requires_scan());

        // Test % to * conversion only
        let matcher = PatternMatcher::from_like_pattern("user%");
        assert_eq!(matcher.get_pattern(), "user*"); // % becomes *
        assert!(matcher.requires_scan());

        let matcher = PatternMatcher::from_like_pattern("exactmatch");
        assert_eq!(matcher.get_pattern(), "exactmatch");
        assert!(!matcher.requires_scan());
    }

    #[test]
    fn test_glob_matching() {
        assert!(glob_match("user_*", "user_123"));
        assert!(glob_match("user_*", "user_abc"));
        assert!(!glob_match("user_*", "admin_123"));

        assert!(glob_match("user_?", "user_1"));
        assert!(!glob_match("user_?", "user_12"));

        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "Exact"));
    }

    #[test]
    fn test_scan_conditions_extraction() {
        let conditions = vec![
            PushableCondition {
                column_name: "key".to_string(),
                operator: ComparisonOperator::Like,
                value: "user_%".to_string(),
            },
            PushableCondition {
                column_name: "key".to_string(),
                operator: ComparisonOperator::Equal,
                value: "exact_key".to_string(),
            },
        ];

        let scan_conditions = extract_scan_conditions(&conditions);
        assert!(scan_conditions.has_optimizable_conditions());
        assert_eq!(scan_conditions.pattern_conditions.len(), 1);
        assert_eq!(scan_conditions.exact_conditions.len(), 1);
    }
}
