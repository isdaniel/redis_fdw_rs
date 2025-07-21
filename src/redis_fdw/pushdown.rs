/// WHERE clause pushdown implementation for Redis FDW
/// This module provides functionality to analyze WHERE clauses and push down
/// supported conditions to Redis for better performance.

use std::collections::HashMap;
use pgrx::{prelude::*, pg_sys};
use crate::redis_fdw::state::RedisTableType;

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
    Equal,           // =
    NotEqual,        // <>
    Like,            // LIKE
    NotLike,         // NOT LIKE
    In,              // IN (...)
    NotIn,           // NOT IN (...)
}

/// Result of WHERE clause analysis
#[derive(Debug, Clone)]
pub struct PushdownAnalysis {
    pub pushable_conditions: Vec<PushableCondition>,
    pub remaining_conditions: Vec<String>, // Conditions that can't be pushed down
    pub can_optimize: bool,
}

/// Analyzes WHERE clauses and determines what can be pushed down to Redis
pub struct WhereClausePushdown;

impl WhereClausePushdown {
    /// Analyze the WHERE clauses and determine what can be pushed down
    pub unsafe fn analyze_scan_clauses(
        scan_clauses: *mut pg_sys::List,
        table_type: &RedisTableType,
    ) -> PushdownAnalysis {
        let mut analysis = PushdownAnalysis {
            pushable_conditions: Vec::new(),
            remaining_conditions: Vec::new(),
            can_optimize: false,
        };

        if scan_clauses.is_null() {
            return analysis;
        }

        // Extract clauses from the list
        let clauses = Self::extract_clauses_from_list(scan_clauses);
        
        for clause in clauses {
            if let Some(condition) = Self::analyze_expression(clause, table_type) {
                analysis.pushable_conditions.push(condition);
                analysis.can_optimize = true;
            } else {
                // Store the unpushable clause for later evaluation
                analysis.remaining_conditions.push(format!("unpushable_clause_{}", clause as u64));
            }
        }

        analysis
    }

    /// Extract individual clauses from PostgreSQL List
    unsafe fn extract_clauses_from_list(scan_clauses: *mut pg_sys::List) -> Vec<*mut pg_sys::Node> {
        let mut clauses = Vec::new();
        
        if scan_clauses.is_null() {
            return clauses;
        }

        let list_length = pg_sys::list_length(scan_clauses);
        for i in 0..list_length {
            let node = pg_sys::list_nth(scan_clauses, i as i32) as *mut pg_sys::Node;
            if !node.is_null() {
                clauses.push(node);
            }
        }

        clauses
    }

    /// Analyze a single expression to see if it can be pushed down
    unsafe fn analyze_expression(
        node: *mut pg_sys::Node,
        table_type: &RedisTableType,
    ) -> Option<PushableCondition> {
        if node.is_null() {
            return None;
        }

        match (*node).type_ {
            pg_sys::NodeTag::T_OpExpr => {
                Self::analyze_op_expr(node as *mut pg_sys::OpExpr, table_type)
            }
            pg_sys::NodeTag::T_ScalarArrayOpExpr => {
                Self::analyze_scalar_array_op_expr(node as *mut pg_sys::ScalarArrayOpExpr, table_type)
            }
            pg_sys::NodeTag::T_RestrictInfo => {
                Self::analyze_restrict_info(node as *mut pg_sys::RestrictInfo, table_type)
            }
            _ => {
                // Other expression types are not supported for pushdown yet
                None
            }
        }
    }

    /// Analyze operator expressions (=, <>, LIKE, etc.)
    unsafe fn analyze_op_expr(
        op_expr: *mut pg_sys::OpExpr,
        table_type: &RedisTableType,
    ) -> Option<PushableCondition> {
        if op_expr.is_null() {
            return None;
        }

        let op_expr = &*op_expr;
        
        // Must have exactly 2 arguments for binary operators
        if pg_sys::list_length(op_expr.args) != 2 {
            return None;
        }

        let left_arg = pg_sys::list_nth(op_expr.args, 0) as *mut pg_sys::Node;
        let right_arg = pg_sys::list_nth(op_expr.args, 1) as *mut pg_sys::Node;

        // Extract column name and value
        let (column_name, value) = Self::extract_column_and_value(left_arg, right_arg)?;

        // Determine operator type based on operator OID
        let operator = Self::get_operator_from_oid(op_expr.opno)?;

        // Check if this condition is suitable for the table type
        if Self::is_condition_pushable( &operator, table_type) {
            Some(PushableCondition {
                column_name,
                operator,
                value,
            })
        } else {
            None
        }
    }

    /// Analyze restrict info nodes (wrapper around actual expressions)
    unsafe fn analyze_restrict_info(
        restrict_info: *mut pg_sys::RestrictInfo,
        table_type: &RedisTableType,
    ) -> Option<PushableCondition> {
        if restrict_info.is_null() {
            return None;
        }

        let restrict_info_ref = &*restrict_info;
        
        // RestrictInfo is a wrapper around the actual clause
        // The actual expression is in the 'clause' field
        let clause = restrict_info_ref.clause as *mut pg_sys::Node;
        
        if clause.is_null() {
            return None;
        }

        // Recursively analyze the wrapped clause
        Self::analyze_expression(clause, table_type)
    }

    /// Analyze scalar array operator expressions (IN, NOT IN)
    unsafe fn analyze_scalar_array_op_expr(
        array_op_expr: *mut pg_sys::ScalarArrayOpExpr,
        table_type: &RedisTableType,
    ) -> Option<PushableCondition> {
        if array_op_expr.is_null() {
            return None;
        }

        let array_op_expr = &*array_op_expr;
        
        // Must have exactly 2 arguments
        if pg_sys::list_length(array_op_expr.args) != 2 {
            return None;
        }

        let left_arg = pg_sys::list_nth(array_op_expr.args, 0) as *mut pg_sys::Node;
        let right_arg = pg_sys::list_nth(array_op_expr.args, 1) as *mut pg_sys::Node;

        // Extract column name and array values
        let column_name = Self::extract_column_name(left_arg)?;
        let array_values = Self::extract_array_values(right_arg)?;

        // Determine if it's IN or NOT IN
        let operator = if array_op_expr.useOr {
            ComparisonOperator::In
        } else {
            ComparisonOperator::NotIn
        };

        // Join array values for storage
        let value = array_values.join(",");

        if Self::is_condition_pushable(&operator, table_type) {
            Some(PushableCondition {
                column_name,
                operator,
                value,
            })
        } else {
            None
        }
    }

    /// Extract column name and value from binary expression arguments
    unsafe fn extract_column_and_value(
        left_arg: *mut pg_sys::Node,
        right_arg: *mut pg_sys::Node,
    ) -> Option<(String, String)> {
        // Try left as column, right as value
        if let (Some(column), Some(value)) = (
            Self::extract_column_name(left_arg),
            Self::extract_constant_value(right_arg),
        ) {
            info!("Extracted column: {}, value: {}", column, value);
            return Some((column, value));
        }

        // Try right as column, left as value (for cases like '5' = column)
        if let (Some(column), Some(value)) = (
            Self::extract_column_name(right_arg),
            Self::extract_constant_value(left_arg),
        ) {
            return Some((column, value));
        }

        None
    }

    /// Extract column name from a Var node
    unsafe fn extract_column_name(node: *mut pg_sys::Node) -> Option<String> {
        if node.is_null() {
            return None;
        }

        match (*node).type_ {
            pg_sys::NodeTag::T_Var => {
                let var = node as *mut pg_sys::Var;
                let var_ref = &*var;
                
                // Get the attribute name from the relation
                // This is a simplified version - in practice, you'd need to look up
                // the actual column name from the tuple descriptor
                match var_ref.varattno {
                    1 => Some("key".to_string()),    // First column is typically key/field
                    2 => Some("value".to_string()),  // Second column is typically value
                    3 => Some("score".to_string()),  // Third column (for zset) is typically score
                    _ => Some(format!("col_{}", var_ref.varattno)),
                }
            }
            _ => None,
        }
    }

    /// Extract constant value from a Const node
    unsafe fn extract_constant_value(node: *mut pg_sys::Node) -> Option<String> {
        if node.is_null() {
            return None;
        }

        match (*node).type_ {
            pg_sys::NodeTag::T_Const => {
                let const_node = node as *mut pg_sys::Const;
                let const_ref = &*const_node;
                
                if const_ref.constisnull {
                    return Some("NULL".to_string());
                }

                // Convert datum to string based on type
                // This is simplified - in practice, you'd need proper type handling
                match const_ref.consttype {
                    pg_sys::TEXTOID => {
                        // Handle text values
                        if let Some(text_val) = String::from_datum(const_ref.constvalue, false) {
                            Some(text_val)
                        } else {
                            None
                        }
                    }
                    pg_sys::INT4OID => {
                        // Handle integer values
                        if let Some(int_val) = i32::from_datum(const_ref.constvalue, false) {
                            Some(int_val.to_string())
                        } else {
                            None
                        }
                    }
                    pg_sys::FLOAT8OID => {
                        // Handle float values
                        if let Some(float_val) = f64::from_datum(const_ref.constvalue, false) {
                            Some(float_val.to_string())
                        } else {
                            None
                        }
                    }
                    _ => {
                        // For other types, try to convert to string
                        Some(format!("unknown_type_{}", const_ref.consttype))
                    }
                }
            }
            _ => None,
        }
    }

    /// Extract array values from an array constant
    unsafe fn extract_array_values(node: *mut pg_sys::Node) -> Option<Vec<String>> {
        if node.is_null() {
            return None;
        }

        // This is a simplified implementation
        // In practice, you'd need to properly handle array deconstruction
        match (*node).type_ {
            pg_sys::NodeTag::T_Const => {
                // For now, just return a placeholder
                Some(vec!["array_value".to_string()])
            }
            _ => None,
        }
    }

    /// Determine operator type from PostgreSQL operator OID
    pub unsafe fn get_operator_from_oid(op_oid: pg_sys::Oid) -> Option<ComparisonOperator> {
        let oid_val = op_oid.to_u32();
        match oid_val {
            98 => Some(ComparisonOperator::Equal),     // text = text
            531 => Some(ComparisonOperator::NotEqual), // text <> text
            664 => Some(ComparisonOperator::Like),     // text LIKE text  
            665 => Some(ComparisonOperator::NotLike),  // text NOT LIKE text
            _ => {
                // Look up operator name from system catalog
                Self::lookup_operator_name(op_oid)
            }
        }
    }

    /// Look up operator name from pg_operator
    unsafe fn lookup_operator_name(op_oid: pg_sys::Oid) -> Option<ComparisonOperator> {
        // This would require accessing PostgreSQL's system catalogs
        // For now, return None for unknown operators
        log!("Unknown operator OID: {}", op_oid.to_u32());
        None
    }

    /// Check if a condition can be pushed down for the given table type
    pub fn is_condition_pushable(
        operator: &ComparisonOperator,
        table_type: &RedisTableType,
    ) -> bool {
        match table_type {
            RedisTableType::Hash(_) => {
                matches!(operator, ComparisonOperator::Equal | ComparisonOperator::In)
            }
            RedisTableType::List(_) => {
                matches!(operator, ComparisonOperator::Equal | ComparisonOperator::Like)
            }
            RedisTableType::Set(_) => {
                matches!(operator, ComparisonOperator::Equal | ComparisonOperator::In)
            }
            RedisTableType::ZSet(_) => {
                matches!(operator, ComparisonOperator::Equal | ComparisonOperator::In)
            }
            RedisTableType::String(_) => {
                matches!(operator, ComparisonOperator::Equal)
            }
            RedisTableType::None => false,
        }
    }

    /// Apply pushdown conditions to Redis query
    pub fn apply_pushdown_to_redis(
        conditions: &[PushableCondition],
        table_type: &RedisTableType,
        conn: &mut redis::Connection,
        key_prefix: &str,
    ) -> Result<Vec<String>, redis::RedisError> {
        if conditions.is_empty() {
            // No conditions to push down, load all data
            return Self::load_all_data(table_type, conn, key_prefix);
        }
        
        match table_type {
            RedisTableType::Hash(_) => {
                Self::apply_hash_pushdown(conditions, conn, key_prefix)
            }
            RedisTableType::List(_) => {
                Self::apply_list_pushdown(conditions, conn, key_prefix)
            }
            RedisTableType::Set(_) => {
                Self::apply_set_pushdown(conditions, conn, key_prefix)
            }
            RedisTableType::ZSet(_) => {
                Self::apply_zset_pushdown(conditions, conn, key_prefix)
            }
            RedisTableType::String(_) => {
                Self::apply_string_pushdown(conditions, conn, key_prefix)
            }
            RedisTableType::None => Ok(vec![]),
        }
    }

    /// Load all data when no pushdown is possible
    fn load_all_data(
        table_type: &RedisTableType,
        conn: &mut redis::Connection,
        key_prefix: &str,
    ) -> Result<Vec<String>, redis::RedisError> {
        match table_type {
            RedisTableType::Hash(_) => {
                let hash_data: HashMap<String, String> = redis::cmd("HGETALL")
                    .arg(key_prefix)
                    .query(conn)?;
                Ok(hash_data.into_iter()
                    .flat_map(|(k, v)| vec![k, v])
                    .collect())
            }
            RedisTableType::List(_) => {
                let list_data: Vec<String> = redis::cmd("LRANGE")
                    .arg(key_prefix)
                    .arg(0)
                    .arg(-1)
                    .query(conn)?;
                Ok(list_data)
            }
            RedisTableType::Set(_) => {
                let set_data: Vec<String> = redis::cmd("SMEMBERS")
                    .arg(key_prefix)
                    .query(conn)?;
                Ok(set_data)
            }
            RedisTableType::ZSet(_) => {
                let zset_data: Vec<String> = redis::cmd("ZRANGE")
                    .arg(key_prefix)
                    .arg(0)
                    .arg(-1)
                    .arg("WITHSCORES")
                    .query(conn)?;
                Ok(zset_data)
            }
            RedisTableType::String(_) => {
                let string_data: Option<String> = redis::cmd("GET")
                    .arg(key_prefix)
                    .query(conn)?;
                Ok(string_data.map(|s| vec![s]).unwrap_or_default())
            }
            RedisTableType::None => Ok(vec![]),
        }
    }

    /// Apply pushdown for hash tables
    fn apply_hash_pushdown(
        conditions: &[PushableCondition],
        conn: &mut redis::Connection,
        key_prefix: &str,
    ) -> Result<Vec<String>, redis::RedisError> {
        // For hash tables, we can optimize field-specific queries
        for condition in conditions {
            match condition.operator {
                ComparisonOperator::Equal => {
                    let value: Option<String> = redis::cmd("HGET")
                        .arg(key_prefix)
                        .arg(&condition.value)
                        .query(conn)?;

                    return if let Some(v) = value {
                        Ok(vec![condition.value.clone(), v])
                    } else {
                        Ok(vec![])
                    };
                }
                ComparisonOperator::In => {
                    // HMGET for multiple fields
                    let fields: Vec<&str> = condition.value.split(',').collect();
                    let values: Vec<Option<String>> = redis::cmd("HMGET")
                        .arg(key_prefix)
                        .arg(&fields)
                        .query(conn)?;
                    
                    let mut result = Vec::new();
                    for (i, value) in values.iter().enumerate() {
                        if let Some(v) = value {
                            result.push(fields[i].to_string());
                            result.push(v.clone());
                        }
                    }
                    return Ok(result);
                }
                _ => {} // Fall back to full scan
            }
        }

        // Fall back to loading all data
        Self::load_all_data(&RedisTableType::Hash(Default::default()), conn, key_prefix)
    }

    /// Apply pushdown for list tables
    fn apply_list_pushdown(
        _conditions: &[PushableCondition],
        conn: &mut redis::Connection,
        key_prefix: &str,
    ) -> Result<Vec<String>, redis::RedisError> {
        // Lists don't have efficient filtering in Redis
        // Fall back to loading all data
        Self::load_all_data(&RedisTableType::List(Default::default()), conn, key_prefix)
    }

    /// Apply pushdown for set tables
    fn apply_set_pushdown(
        conditions: &[PushableCondition],
        conn: &mut redis::Connection,
        key_prefix: &str,
    ) -> Result<Vec<String>, redis::RedisError> {
        // For sets, we can check membership efficiently
        for condition in conditions {
            match condition.operator {
                ComparisonOperator::Equal => {
                    // SISMEMBER for specific member
                    let exists: bool = redis::cmd("SISMEMBER")
                        .arg(key_prefix)
                        .arg(&condition.value)
                        .query(conn)?;
                    
                    return if exists {
                        Ok(vec![condition.value.clone()])
                    } else {
                        Ok(vec![])
                    };
                }
                ComparisonOperator::In => {
                    // Check multiple members
                    let members: Vec<&str> = condition.value.split(',').collect();
                    let mut result = Vec::new();
                    
                    for member in members {
                        let exists: bool = redis::cmd("SISMEMBER")
                            .arg(key_prefix)
                            .arg(member)
                            .query(conn)?;
                        
                        if exists {
                            result.push(member.to_string());
                        }
                    }
                    return Ok(result);
                }
                _ => {} // Fall back to full scan
            }
        }

        // Fall back to loading all data
        Self::load_all_data(&RedisTableType::Set(Default::default()), conn, key_prefix)
    }

    /// Apply pushdown for sorted set tables
    fn apply_zset_pushdown(
        _conditions: &[PushableCondition],
        conn: &mut redis::Connection,
        key_prefix: &str,
    ) -> Result<Vec<String>, redis::RedisError> {
        // ZSets could support score-based range queries
        // For now, fall back to loading all data
        Self::load_all_data(&RedisTableType::ZSet(Default::default()), conn, key_prefix)
    }

    /// Apply pushdown for string tables
    fn apply_string_pushdown(
        conditions: &[PushableCondition],
        conn: &mut redis::Connection,
        key_prefix: &str,
    ) -> Result<Vec<String>, redis::RedisError> {
        // String tables can only be checked for exact value match
        for condition in conditions {
            let value: Option<String> = redis::cmd("GET")
                .arg(key_prefix)
                .query(conn)?;
            
            return if let Some(v) = value {
                if v == condition.value {
                    Ok(vec![v])
                } else {
                    Ok(vec![])
                }
            } else {
                Ok(vec![])
            };
        }

        // Fall back to loading all data
        Self::load_all_data(&RedisTableType::String(Default::default()), conn, key_prefix)
    }
}
