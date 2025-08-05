/// WHERE clause pushdown implementation for Redis FDW
/// This module provides functionality to analyze WHERE clauses and push down
/// supported conditions to Redis for better performance.
use crate::{
    query::pushdown_types::{ComparisonOperator, PushableCondition, PushdownAnalysis},
    tables::types::RedisTableType,
    utils::{
        cell::Cell,
        utils::{relation_get_descr, tuple_desc_attr},
    },
};
use pgrx::{pg_sys, prelude::*};

/// Analyzes WHERE clauses and determines what can be pushed down to Redis
pub struct WhereClausePushdown;

impl WhereClausePushdown {
    /// Analyze the WHERE clauses and determine what can be pushed down
    pub unsafe fn analyze_scan_clauses(
        scan_clauses: *mut pg_sys::List,
        table_type: &RedisTableType,
        relation: pg_sys::Relation,
    ) -> PushdownAnalysis {
        let mut analysis = PushdownAnalysis {
            pushable_conditions: Vec::new(),
            can_optimize: false,
        };

        if scan_clauses.is_null() {
            return analysis;
        }

        // Extract clauses from the list
        let clauses = Self::extract_clauses_from_list(scan_clauses);

        for clause in clauses {
            if let Some(condition) = Self::analyze_expression(clause, table_type, relation) {
                analysis.pushable_conditions.push(condition);
                analysis.can_optimize = true;
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
            let node = pg_sys::list_nth(scan_clauses, i) as *mut pg_sys::Node;
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
        relation: pg_sys::Relation,
    ) -> Option<PushableCondition> {
        if node.is_null() {
            return None;
        }
        //info!("Analyzing expression (*node).type_: {:?}", (*node).type_);
        match (*node).type_ {
            pg_sys::NodeTag::T_OpExpr => {
                Self::analyze_op_expr(node as *mut pg_sys::OpExpr, table_type, relation)
            }
            pg_sys::NodeTag::T_ScalarArrayOpExpr => Self::analyze_scalar_array_op_expr(
                node as *mut pg_sys::ScalarArrayOpExpr,
                table_type,
                relation,
            ),
            pg_sys::NodeTag::T_RestrictInfo => {
                Self::analyze_restrict_info(node as *mut pg_sys::RestrictInfo, table_type, relation)
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
        relation: pg_sys::Relation,
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
        let (column_name, value) = Self::extract_column_and_value(left_arg, right_arg, relation)?;
        // Determine operator type based on operator OID
        let operator = Self::get_operator_from_oid(op_expr.opno)?;

        // Check if this condition is suitable for the table type
        if table_type.supports_pushdown(&operator) {
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
        relation: pg_sys::Relation,
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
        Self::analyze_expression(clause, table_type, relation)
    }

    /// Analyze scalar array operator expressions (IN, NOT IN)
    unsafe fn analyze_scalar_array_op_expr(
        array_op_expr: *mut pg_sys::ScalarArrayOpExpr,
        table_type: &RedisTableType,
        relation: pg_sys::Relation,
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

        // Extract column name
        let column_name = Self::extract_column_name(left_arg, relation)?;

        // Determine if it's IN or NOT IN
        let operator = if array_op_expr.useOr {
            ComparisonOperator::In
        } else {
            ComparisonOperator::NotIn
        };

        // Check if this condition is suitable for the table type
        if !table_type.supports_pushdown(&operator) {
            return None;
        }

        // Try to extract array values using a simpler approach
        if let Some(array_values) = Self::extract_array_values(right_arg) {
            let value = array_values.join(",");
            Some(PushableCondition {
                column_name,
                operator,
                value,
            })
        } else {
            // If we can't safely extract the array, disable pushdown for safety
            log!("Could not safely extract array values, disabling pushdown for this IN clause");
            None
        }
    }

    /// Safer array extraction that avoids memory corruption issues
    unsafe fn extract_array_values(node: *mut pg_sys::Node) -> Option<Vec<String>> {
        if node.is_null() {
            return None;
        }

        log!("Analyzing scalar array (*node).type_: {:?}", (*node).type_);

        match (*node).type_ {
            // Handle simple array expressions like ARRAY['a', 'b', 'c']
            pg_sys::NodeTag::T_ArrayExpr => {
                let array_expr = node as *mut pg_sys::ArrayExpr;
                let array_expr_ref = &*array_expr;

                let mut result = Vec::new();
                let list_length = pg_sys::list_length(array_expr_ref.elements);

                for i in 0..list_length {
                    let elem_node =
                        pg_sys::list_nth(array_expr_ref.elements, i) as *mut pg_sys::Node;
                    if !elem_node.is_null() {
                        if let Some(value) = Self::extract_constant_value(elem_node) {
                            result.push(value);
                        } else {
                            // If we can't extract a value safely, abort
                            log!(
                                "Could not extract array element {}, aborting safe extraction",
                                i
                            );
                            return None;
                        }
                    }
                }

                Some(result)
            }
            pg_sys::NodeTag::T_Const => {
                let const_node = node as *mut pg_sys::Const;
                let const_ref = &*const_node;

                if const_ref.constisnull {
                    return None;
                }

                // Check if this is an array type
                let array_datum = const_ref.constvalue;
                let array_type = const_ref.consttype;

                // Get array element type
                let element_type = pg_sys::get_element_type(array_type);
                if element_type == pg_sys::InvalidOid {
                    log!("Not an array type: {}", array_type);
                    return None;
                }

                // Get proper type information for the element type
                let mut typlen: i16 = 0;
                let mut typbyval: bool = false;
                let mut typalign: i8 = 0;
                let mut nelems: i32 = 0;
                let mut elems: *mut pg_sys::Datum = std::ptr::null_mut();
                let mut nulls: *mut bool = std::ptr::null_mut();

                pg_sys::get_typlenbyvalalign(
                    element_type,
                    &mut typlen,
                    &mut typbyval,
                    &mut typalign,
                );

                // Validate type information
                if typlen == 0 && !typbyval {
                    log!(
                        "Invalid type information for element type: {}",
                        element_type
                    );
                    return None;
                }

                // Convert Datum to ArrayType pointer - ensure proper casting
                let array_ptr = array_datum.cast_mut_ptr::<pg_sys::ArrayType>();
                if array_ptr.is_null() {
                    log!("Failed to convert datum to ArrayType");
                    return None;
                }

                pg_sys::deconstruct_array(
                    array_ptr,
                    element_type,
                    typlen as i32,
                    typbyval,
                    typalign,
                    &mut elems,
                    &mut nulls,
                    &mut nelems,
                );

                if nelems > 10000 || nelems <= 0 || elems.is_null() {
                    log!(
                        r#"Will not extract array values due to safety checks: arrry length ({})
                    1. Array too large, limiting extraction for safety
                    2. Invalid element count, limiting extraction for safety
                    3. Null elements present, limiting extraction for safety"#,
                        nelems
                    );
                    return None;
                }

                let mut result = Vec::new();

                // Extract each element from the array with better error handling
                for i in 0..nelems {
                    let elem_datum = *elems.offset(i as isize);
                    let is_null = if nulls.is_null() {
                        false
                    } else {
                        *nulls.offset(i as isize)
                    };

                    if is_null {
                        result.push("NULL".to_string());
                    } else {
                        // Use safer conversion - remove panic handling for now
                        if let Some(cell) =
                            Cell::from_polymorphic_datum(elem_datum, is_null, element_type)
                        {
                            result.push(cell.to_string());
                        } else {
                            log!(
                                "Could not convert element {} to cell, aborting extraction",
                                i
                            );
                            // For safety, abort the entire extraction if any element fails
                            return None;
                        }
                    }
                }

                Some(result)
            }
            _ => {
                log!(
                    "Unsupported node type for safe array extraction: {:?}",
                    (*node).type_
                );
                None
            }
        }
    }

    /// Extract column name and value from binary expression arguments
    unsafe fn extract_column_and_value(
        left_arg: *mut pg_sys::Node,
        right_arg: *mut pg_sys::Node,
        relation: pg_sys::Relation,
    ) -> Option<(String, String)> {
        // Try left as column, right as value
        if let (Some(column), Some(value)) = (
            Self::extract_column_name(left_arg, relation),
            Self::extract_constant_value(right_arg),
        ) {
            return Some((column, value));
        }

        // Try right as column, left as value (for cases like '5' = column)
        if let (Some(column), Some(value)) = (
            Self::extract_column_name(right_arg, relation),
            Self::extract_constant_value(left_arg),
        ) {
            return Some((column, value));
        }

        None
    }

    /// Extract column name from a Var node
    unsafe fn extract_column_name(
        node: *mut pg_sys::Node,
        relation: pg_sys::Relation,
    ) -> Option<String> {
        if node.is_null() {
            return None;
        }

        match (*node).type_ {
            pg_sys::NodeTag::T_Var => {
                let var = node as *mut pg_sys::Var;
                let var_ref = *var;

                let tupdesc = relation_get_descr(relation);
                if !tupdesc.is_null() {
                    let attr_no = var_ref.varattno;
                    // PostgreSQL attribute numbers are 1-based, but our array access is 0-based
                    if attr_no > 0 && (attr_no as usize) <= (*tupdesc).natts as usize {
                        let attr_idx = (attr_no - 1) as usize;
                        let attr = tuple_desc_attr(tupdesc, attr_idx);
                        if !attr.is_null() {
                            let attr_ref = &*attr;
                            let column_name = pgrx::name_data_to_str(&attr_ref.attname);
                            return Some(column_name.to_string());
                        }
                    }
                }

                None
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
                Cell::from_polymorphic_datum(
                    const_ref.constvalue,
                    const_ref.constisnull,
                    const_ref.consttype,
                )
                .map(|val| val.to_string())
            }
            _ => None,
        }
    }

    /// Determine operator type from PostgreSQL operator OID
    unsafe fn get_operator_from_oid(op_oid: pg_sys::Oid) -> Option<ComparisonOperator> {
        let oid_val = op_oid.to_u32();
        match oid_val {
            98 => Some(ComparisonOperator::Equal),     // text = text
            531 => Some(ComparisonOperator::NotEqual), // text <> text
            1209 => Some(ComparisonOperator::Like),    // text LIKE text
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
}
