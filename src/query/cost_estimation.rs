/// Cost estimation module for Redis FDW
///
/// This module provides execution plan cost estimation logic for PostgreSQL's
/// query planner. It gathers Redis statistics and calculates appropriate
/// row estimates and cost values based on:
/// - Redis key cardinality (DBSIZE, SCAN count)
/// - Data type specific cardinality (LLEN, HLEN, SCARD, ZCARD, XLEN)
/// - WHERE clause pushdown analysis
/// - LIMIT/OFFSET information
///
/// The cost model aims to provide accurate estimates to help PostgreSQL's
/// planner make optimal join ordering and access method decisions.
use crate::{query::pushdown_types::PushdownAnalysis, tables::types::RedisTableType};
use redis::{cmd, ConnectionLike};

/// Default cost constants for Redis FDW operations
pub mod costs {
    /// Base cost to establish/acquire connection from pool
    pub const CONNECTION_OVERHEAD: f64 = 1.0;

    /// Cost per network round-trip to Redis
    pub const NETWORK_ROUND_TRIP: f64 = 10.0;

    /// Cost per row transferred over the network from Redis
    pub const NETWORK_TRANSFER_PER_ROW: f64 = 0.005;

    /// Cost to process one row/tuple
    pub const CPU_TUPLE_COST: f64 = 0.01;

    /// Cost to evaluate one operator/function
    pub const CPU_OPERATOR_COST: f64 = 0.0025;

    /// Default estimated rows when we can't determine actual count
    pub const DEFAULT_ROW_ESTIMATE: f64 = 1000.0;

    /// Minimum row estimate (never go below this)
    pub const MIN_ROW_ESTIMATE: f64 = 1.0;

    /// Selectivity estimate for equality conditions
    pub const EQUALITY_SELECTIVITY: f64 = 0.1;

    /// Selectivity estimate for range conditions
    pub const RANGE_SELECTIVITY: f64 = 0.33;

    /// Selectivity estimate for LIKE conditions
    pub const LIKE_SELECTIVITY: f64 = 0.25;

    /// Number of keys to request per SCAN iteration for sampling
    pub const SCAN_SAMPLE_SIZE: u64 = 100;

    /// Minimum selectivity when SCAN sample finds no matches but DB is non-empty
    pub const MIN_SCAN_SELECTIVITY: f64 = 0.01;
}

/// Statistics gathered from Redis for cost estimation
#[derive(Debug, Clone, Default)]
pub struct RedisStatistics {
    /// Total number of keys in the database (from DBSIZE)
    pub db_key_count: Option<u64>,

    /// Number of keys matching the table's key prefix pattern
    pub matching_key_count: Option<u64>,

    /// Cardinality of the specific key (for single-key tables)
    /// e.g., HLEN for hash, LLEN for list, etc.
    pub key_cardinality: Option<u64>,

    /// Whether statistics gathering was successful
    pub stats_available: bool,
}

impl RedisStatistics {
    /// Create empty statistics (when connection not available)
    pub fn empty() -> Self {
        Self::default()
    }

    /// Create statistics with only db_key_count
    #[cfg(test)]
    pub fn with_db_size(db_size: u64) -> Self {
        Self {
            db_key_count: Some(db_size),
            stats_available: true,
            ..Default::default()
        }
    }
}

/// Cost estimation results for the query planner
#[derive(Debug, Clone)]
pub struct CostEstimate {
    /// Estimated number of rows to be returned
    pub rows: f64,

    /// Startup cost (cost before first row can be returned)
    pub startup_cost: f64,

    /// Total cost (cost to return all rows)
    pub total_cost: f64,

    /// Width estimate (average bytes per row)
    pub width: i32,
}

impl Default for CostEstimate {
    fn default() -> Self {
        Self {
            rows: costs::DEFAULT_ROW_ESTIMATE,
            startup_cost: costs::NETWORK_ROUND_TRIP,
            total_cost: costs::DEFAULT_ROW_ESTIMATE * costs::CPU_TUPLE_COST
                + costs::NETWORK_ROUND_TRIP,
            width: 100, // Default estimated width
        }
    }
}

/// Cost estimator for Redis FDW operations
pub struct CostEstimator<'a> {
    pub table_type: &'a RedisTableType,
    pub key_prefix: &'a str,
    pub pushdown_analysis: Option<&'a PushdownAnalysis>,
}

impl<'a> CostEstimator<'a> {
    /// Create a new cost estimator
    pub fn new(
        table_type: &'a RedisTableType,
        key_prefix: &'a str,
        pushdown_analysis: Option<&'a PushdownAnalysis>,
    ) -> Self {
        Self {
            table_type,
            key_prefix,
            pushdown_analysis,
        }
    }

    /// Gather statistics from Redis connection
    pub fn gather_statistics(&self, conn: &mut dyn ConnectionLike) -> RedisStatistics {
        let mut stats = RedisStatistics::default();

        // Get overall database size
        if let Ok(db_size) = cmd("DBSIZE").query::<u64>(conn) {
            stats.db_key_count = Some(db_size);
            stats.stats_available = true;
        }

        // Get type-specific cardinality
        stats.key_cardinality = self.get_key_cardinality(conn);

        // Estimate matching keys if we have a prefix
        if !self.key_prefix.is_empty() {
            stats.matching_key_count = self.estimate_matching_keys(conn);
        }

        stats
    }

    /// Get cardinality for the specific Redis key based on data type
    fn get_key_cardinality(&self, conn: &mut dyn ConnectionLike) -> Option<u64> {
        if self.key_prefix.is_empty() {
            return None;
        }

        match self.table_type {
            RedisTableType::String(_) => {
                // String type has cardinality of 1 per key
                // Check if key exists
                if let Ok(exists) = cmd("EXISTS").arg(self.key_prefix).query::<i32>(conn) {
                    Some(exists.max(0) as u64)
                } else {
                    Some(1) // Assume exists
                }
            }
            RedisTableType::Hash(_) => {
                // HLEN returns number of fields in hash
                cmd("HLEN").arg(self.key_prefix).query::<u64>(conn).ok()
            }
            RedisTableType::List(_) => {
                // LLEN returns list length
                cmd("LLEN").arg(self.key_prefix).query::<u64>(conn).ok()
            }
            RedisTableType::Set(_) => {
                // SCARD returns set cardinality
                cmd("SCARD").arg(self.key_prefix).query::<u64>(conn).ok()
            }
            RedisTableType::ZSet(_) => {
                // ZCARD returns sorted set cardinality
                cmd("ZCARD").arg(self.key_prefix).query::<u64>(conn).ok()
            }
            RedisTableType::Stream(_) => {
                // XLEN returns stream length
                cmd("XLEN").arg(self.key_prefix).query::<u64>(conn).ok()
            }
            RedisTableType::None => None,
        }
    }

    /// Estimate number of keys matching the prefix pattern
    /// Uses a single SCAN iteration to sample, then extrapolates from DBSIZE
    fn estimate_matching_keys(&self, conn: &mut dyn ConnectionLike) -> Option<u64> {
        let pattern = if self.key_prefix.contains('*') {
            self.key_prefix.to_string()
        } else {
            format!("{}*", self.key_prefix)
        };

        let sample_size = costs::SCAN_SAMPLE_SIZE;
        let result: Result<(i64, Vec<String>), _> = cmd("SCAN")
            .arg(0)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(sample_size)
            .query(conn);

        match result {
            Ok((cursor, keys)) => {
                let count = keys.len() as u64;
                if cursor == 0 {
                    return Some(count);
                }
                if let Ok(db_size) = cmd("DBSIZE").query::<u64>(conn) {
                    if count == 0 {
                        // No matches in sample but DB is non-empty — use minimum selectivity
                        return Some((db_size as f64 * costs::MIN_SCAN_SELECTIVITY).max(1.0) as u64);
                    }
                    let sample_ratio = count as f64 / sample_size as f64;
                    Some((db_size as f64 * sample_ratio).max(count as f64) as u64)
                } else {
                    Some(count)
                }
            }
            Err(_) => None,
        }
    }

    /// Calculate cost estimate based on gathered statistics
    pub fn calculate_cost(&self, stats: &RedisStatistics) -> CostEstimate {
        let base_rows = self.estimate_base_rows(stats);
        let adjusted_rows = self.apply_selectivity(base_rows);
        let final_rows = self.apply_limit_offset(adjusted_rows);

        let (startup_cost, total_cost) = self.calculate_costs(final_rows, base_rows);

        CostEstimate {
            rows: final_rows.max(costs::MIN_ROW_ESTIMATE),
            startup_cost,
            total_cost,
            width: self.estimate_row_width(),
        }
    }

    /// Estimate base number of rows before applying selectivity
    fn estimate_base_rows(&self, stats: &RedisStatistics) -> f64 {
        // Priority order for estimation:
        // 1. Key cardinality (most accurate for single-key tables)
        // 2. Matching key count (for prefix patterns)
        // 3. Database size (fallback)
        // 4. Default estimate

        if let Some(cardinality) = stats.key_cardinality {
            return cardinality as f64;
        }

        if let Some(matching) = stats.matching_key_count {
            // For multi-key patterns, estimate based on type
            return match self.table_type {
                RedisTableType::String(_) => matching as f64,
                RedisTableType::Hash(_) => (matching * 10) as f64, // Assume avg 10 fields
                RedisTableType::List(_) => (matching * 100) as f64, // Assume avg 100 items
                RedisTableType::Set(_) => (matching * 50) as f64,
                RedisTableType::ZSet(_) => (matching * 50) as f64,
                RedisTableType::Stream(_) => (matching * 1000) as f64,
                RedisTableType::None => matching as f64,
            };
        }

        if let Some(db_size) = stats.db_key_count {
            // Use a fraction of total keys as estimate
            return (db_size as f64 * 0.1).max(costs::MIN_ROW_ESTIMATE);
        }

        costs::DEFAULT_ROW_ESTIMATE
    }

    /// Apply selectivity based on pushdown conditions
    fn apply_selectivity(&self, rows: f64) -> f64 {
        let Some(analysis) = self.pushdown_analysis else {
            return rows;
        };

        if !analysis.can_optimize {
            return rows;
        }

        let mut selectivity = 1.0;

        for condition in &analysis.pushable_conditions {
            use crate::query::pushdown_types::ComparisonOperator;

            let condition_selectivity = match condition.operator {
                ComparisonOperator::Equal => costs::EQUALITY_SELECTIVITY,
                ComparisonOperator::NotEqual => 1.0 - costs::EQUALITY_SELECTIVITY,
                ComparisonOperator::Like => costs::LIKE_SELECTIVITY,
                ComparisonOperator::In => costs::EQUALITY_SELECTIVITY * 2.0, // Slightly more selective
                ComparisonOperator::NotIn => 1.0 - costs::EQUALITY_SELECTIVITY * 2.0,
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual => costs::RANGE_SELECTIVITY,
            };

            // Combine selectivities (assuming independence)
            selectivity *= condition_selectivity;
        }

        (rows * selectivity).max(costs::MIN_ROW_ESTIMATE)
    }

    /// Apply LIMIT/OFFSET constraints to row estimate
    fn apply_limit_offset(&self, rows: f64) -> f64 {
        let Some(analysis) = self.pushdown_analysis else {
            return rows;
        };

        let Some(ref limit_info) = analysis.limit_offset else {
            return rows;
        };

        let mut result = rows;

        // Apply OFFSET first
        if let Some(offset) = limit_info.offset {
            result = (result - offset as f64).max(0.0);
        }

        // Apply LIMIT
        if let Some(limit) = limit_info.limit {
            result = result.min(limit as f64);
        }

        result.max(costs::MIN_ROW_ESTIMATE)
    }

    /// Calculate startup and total costs
    fn calculate_costs(&self, final_rows: f64, base_rows: f64) -> (f64, f64) {
        // Startup cost: connection + initial Redis command
        let mut startup_cost = costs::CONNECTION_OVERHEAD + costs::NETWORK_ROUND_TRIP;

        // Additional startup cost if pushdown conditions need to be evaluated
        if let Some(analysis) = self.pushdown_analysis {
            if !analysis.pushable_conditions.is_empty() {
                startup_cost +=
                    analysis.pushable_conditions.len() as f64 * costs::CPU_OPERATOR_COST;
            }
        }

        // Total cost: startup + per-row processing
        let total_cost = startup_cost
            + (final_rows * costs::CPU_TUPLE_COST)
            + self.estimate_network_cost(base_rows, final_rows);

        (startup_cost, total_cost)
    }

    /// Estimate network transfer cost
    fn estimate_network_cost(&self, base_rows: f64, final_rows: f64) -> f64 {
        // If pushdown reduces rows significantly, we save on network transfer
        let transfer_rows = if let Some(analysis) = self.pushdown_analysis {
            if analysis.can_optimize {
                // Some filtering happens at Redis side
                (base_rows * 0.5 + final_rows * 0.5).min(base_rows)
            } else {
                base_rows
            }
        } else {
            base_rows
        };

        // Estimate based on rows * average_width
        let width = self.estimate_row_width() as f64;
        let bytes = transfer_rows * width;

        // Cost per KB of network transfer
        bytes / 1024.0 * 0.01
    }

    /// Estimate average row width based on table type
    fn estimate_row_width(&self) -> i32 {
        match self.table_type {
            RedisTableType::String(_) => 100, // key + value
            RedisTableType::Hash(_) => 150,   // key + field + value
            RedisTableType::List(_) => 50,    // index + value
            RedisTableType::Set(_) => 50,     // member
            RedisTableType::ZSet(_) => 60,    // member + score
            RedisTableType::Stream(_) => 200, // id + multiple fields
            RedisTableType::None => 100,
        }
    }

    /// Quick estimation without Redis connection (for planning phase)
    pub fn estimate_without_connection(&self) -> CostEstimate {
        let stats = RedisStatistics::empty();
        self.calculate_cost(&stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tables::implementations::RedisStringTable;

    #[test]
    fn test_cost_estimate_default() {
        let estimate = CostEstimate::default();
        assert_eq!(estimate.rows, costs::DEFAULT_ROW_ESTIMATE);
        assert!(estimate.startup_cost > 0.0);
        assert!(estimate.total_cost > estimate.startup_cost);
    }

    #[test]
    fn test_redis_statistics_empty() {
        let stats = RedisStatistics::empty();
        assert!(!stats.stats_available);
        assert!(stats.db_key_count.is_none());
    }

    #[test]
    fn test_redis_statistics_with_db_size() {
        let stats = RedisStatistics::with_db_size(5000);
        assert!(stats.stats_available);
        assert_eq!(stats.db_key_count, Some(5000));
    }

    #[test]
    fn test_cost_estimator_without_connection() {
        let table_type = RedisTableType::String(RedisStringTable::new());
        let estimator = CostEstimator::new(&table_type, "test_key", None);

        let estimate = estimator.estimate_without_connection();
        assert!(estimate.rows >= costs::MIN_ROW_ESTIMATE);
        assert!(estimate.startup_cost > 0.0);
        assert!(estimate.total_cost >= estimate.startup_cost);
    }

    #[test]
    fn test_estimate_base_rows_with_cardinality() {
        let table_type = RedisTableType::String(RedisStringTable::new());
        let estimator = CostEstimator::new(&table_type, "test_key", None);

        let stats = RedisStatistics {
            key_cardinality: Some(500),
            ..Default::default()
        };

        let rows = estimator.estimate_base_rows(&stats);
        assert_eq!(rows, 500.0);
    }

    #[test]
    fn test_estimate_row_width() {
        let string_type = RedisTableType::String(RedisStringTable::new());
        let estimator = CostEstimator::new(&string_type, "key", None);
        let width = estimator.estimate_row_width();
        assert!(width > 0);

        // Hash should have larger width
        let hash_type = RedisTableType::Hash(crate::tables::implementations::RedisHashTable::new());
        let hash_estimator = CostEstimator::new(&hash_type, "key", None);
        let hash_width = hash_estimator.estimate_row_width();
        assert!(hash_width > width);
    }

    #[test]
    fn test_apply_limit_offset() {
        use crate::query::limit::LimitOffsetInfo;
        use crate::query::pushdown_types::PushdownAnalysis;

        let table_type = RedisTableType::String(RedisStringTable::new());

        // Test with LIMIT
        let mut limit_info = LimitOffsetInfo::new();
        limit_info.limit = Some(10);

        let analysis = PushdownAnalysis {
            pushable_conditions: vec![],
            can_optimize: false,
            limit_offset: Some(limit_info),
        };

        let estimator = CostEstimator::new(&table_type, "key", Some(&analysis));
        let rows = estimator.apply_limit_offset(1000.0);
        assert_eq!(rows, 10.0);

        // Test with LIMIT and OFFSET
        let mut limit_offset_info = LimitOffsetInfo::new();
        limit_offset_info.limit = Some(10);
        limit_offset_info.offset = Some(5);

        let analysis2 = PushdownAnalysis {
            pushable_conditions: vec![],
            can_optimize: false,
            limit_offset: Some(limit_offset_info),
        };

        let estimator2 = CostEstimator::new(&table_type, "key", Some(&analysis2));
        let rows2 = estimator2.apply_limit_offset(1000.0);
        assert_eq!(rows2, 10.0); // min(1000-5, 10) = 10
    }
}
