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
    Equal,              // =
    NotEqual,           // <>
    Like,               // LIKE
    In,                 // IN (...)
    NotIn,              // NOT IN (...)
    GreaterThan,        // >
    GreaterThanOrEqual, // >=
    LessThan,           // <
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

    /// Set LIMIT/OFFSET information and update optimization flag
    pub fn set_limit_offset(&mut self, limit_offset: Option<LimitOffsetInfo>) {
        let has_limit_constraints = limit_offset.as_ref().is_some_and(|lo| lo.has_constraints());
        self.limit_offset = limit_offset;
        self.can_optimize = !self.pushable_conditions.is_empty() || has_limit_constraints;
    }

    /// Check if any optimizations are possible
    pub fn has_optimizations(&self) -> bool {
        self.can_optimize
    }

    /// Check if LIMIT/OFFSET pushdown is possible
    pub fn has_limit_pushdown(&self) -> bool {
        self.limit_offset
            .as_ref()
            .is_some_and(|lo| lo.has_constraints())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pushdown_analysis_new() {
        let analysis = PushdownAnalysis::new();
        assert!(!analysis.can_optimize);
        assert!(analysis.pushable_conditions.is_empty());
        assert!(analysis.limit_offset.is_none());
        assert!(!analysis.has_optimizations());
        assert!(!analysis.has_limit_pushdown());
    }

    #[test]
    fn test_pushdown_analysis_set_limit_offset_with_limit() {
        let mut analysis = PushdownAnalysis::new();
        let limit_info = LimitOffsetInfo {
            limit: Some(10),
            offset: None,
        };
        analysis.set_limit_offset(Some(limit_info));
        assert!(analysis.has_optimizations());
        assert!(analysis.has_limit_pushdown());
        assert!(analysis.can_optimize);
    }

    #[test]
    fn test_pushdown_analysis_set_limit_offset_none() {
        let mut analysis = PushdownAnalysis::new();
        analysis.set_limit_offset(None);
        assert!(!analysis.has_optimizations());
        assert!(!analysis.has_limit_pushdown());
    }

    #[test]
    fn test_pushdown_analysis_conditions_enable_optimize() {
        let mut analysis = PushdownAnalysis::new();
        analysis.pushable_conditions.push(PushableCondition {
            column_name: "key".to_string(),
            operator: ComparisonOperator::Equal,
            value: "test".to_string(),
        });
        analysis.can_optimize = true;
        assert!(analysis.has_optimizations());
        assert!(!analysis.has_limit_pushdown());
    }

    #[test]
    fn test_pushdown_analysis_set_limit_offset_with_conditions() {
        let mut analysis = PushdownAnalysis::new();
        analysis.pushable_conditions.push(PushableCondition {
            column_name: "member".to_string(),
            operator: ComparisonOperator::Like,
            value: "user:*".to_string(),
        });
        let limit_info = LimitOffsetInfo {
            limit: Some(5),
            offset: Some(2),
        };
        analysis.set_limit_offset(Some(limit_info));
        assert!(analysis.has_optimizations());
        assert!(analysis.has_limit_pushdown());
    }

    #[test]
    fn test_comparison_operator_equality() {
        assert_eq!(ComparisonOperator::Equal, ComparisonOperator::Equal);
        assert_ne!(ComparisonOperator::Equal, ComparisonOperator::NotEqual);
        assert_ne!(ComparisonOperator::Like, ComparisonOperator::In);
    }
}
