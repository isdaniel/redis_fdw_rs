/// WHERE clause pushdown condition types and analysis structures
/// This module contains types used for analyzing and representing WHERE clause conditions

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
}

/// Result of WHERE clause analysis
#[derive(Debug, Clone)]
pub struct PushdownAnalysis {
    pub pushable_conditions: Vec<PushableCondition>,
    pub remaining_conditions: Vec<String>, // Conditions that can't be pushed down
    pub can_optimize: bool,
}
