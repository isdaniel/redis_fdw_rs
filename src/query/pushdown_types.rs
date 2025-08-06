/// WHERE clause pushdown condition types and analysis structures
/// This module contains types used for analyzing and representing WHERE clause conditions

use crate::query::limit::LimitOffsetInfo;

/// Represents a pushable condition from WHERE clause
#[derive(Debug, Clone)]
pub struct PushableCondition {
    pub column_name: String,
    pub operator: ComparisonOperator,
    pub value: String,
}

/// Supported comparison operators for pushdown
#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonOperator {
    Equal,    // =
    NotEqual, // <>
    Like,     // LIKE
    In,       // IN (...)
    NotIn,    // NOT IN (...)
    GreaterThan,      // >
    GreaterThanOrEqual, // >=
    LessThan,         // <
    LessThanOrEqual,    // <=
}

/// Result of WHERE clause analysis with LIMIT/OFFSET pushdown support
#[derive(Debug, Clone)]
pub struct PushdownAnalysis {
    pub pushable_conditions: Vec<PushableCondition>,
    pub can_optimize: bool,
    /// LIMIT/OFFSET information for pushdown optimization
    pub limit_offset: Option<LimitOffsetInfo>,
}

impl PushdownAnalysis {
    /// Create a new empty analysis
    pub fn new() -> Self {
        Self {
            pushable_conditions: Vec::new(),
            can_optimize: false,
            limit_offset: None,
        }
    }

    /// Create analysis with WHERE conditions only
    pub fn with_conditions(conditions: Vec<PushableCondition>) -> Self {
        let can_optimize = !conditions.is_empty();
        Self {
            pushable_conditions: conditions,
            can_optimize,
            limit_offset: None,
        }
    }

    /// Create analysis with both WHERE conditions and LIMIT/OFFSET
    pub fn with_conditions_and_limit(
        conditions: Vec<PushableCondition>,
        limit_offset: Option<LimitOffsetInfo>,
    ) -> Self {
        let can_optimize = !conditions.is_empty() || limit_offset.as_ref().map_or(false, |lo| lo.has_constraints());
        Self {
            pushable_conditions: conditions,
            can_optimize,
            limit_offset,
        }
    }

    /// Set LIMIT/OFFSET information and update optimization flag
    pub fn set_limit_offset(&mut self, limit_offset: Option<LimitOffsetInfo>) {
        let has_limit_constraints = limit_offset.as_ref().map_or(false, |lo| lo.has_constraints());
        self.limit_offset = limit_offset;
        self.can_optimize = !self.pushable_conditions.is_empty() || has_limit_constraints;
    }

    /// Check if any optimizations are possible
    pub fn has_optimizations(&self) -> bool {
        self.can_optimize
    }

    /// Check if LIMIT/OFFSET pushdown is possible
    pub fn has_limit_pushdown(&self) -> bool {
        self.limit_offset.as_ref().map_or(false, |lo| lo.has_constraints())
    }
}
