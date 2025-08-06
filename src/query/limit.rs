use pgrx::{pg_sys, FromDatum, prelude::*};

/// Represents LIMIT and OFFSET constraints that can be pushed down to Redis
#[derive(Debug, Default, Clone, PartialEq)]
pub struct LimitOffsetInfo {
    /// The maximum number of rows to return (LIMIT clause)
    pub limit: Option<usize>,
    /// The number of rows to skip (OFFSET clause) 
    pub offset: Option<usize>,
}

impl LimitOffsetInfo {
    /// Create a new LimitOffsetInfo instance
    pub fn new() -> Self {
        Self::default()
    }

    /// Create LimitOffsetInfo with both limit and offset
    pub fn with_limit_offset(limit: Option<usize>, offset: Option<usize>) -> Self {
        Self { limit, offset }
    }

    /// Check if this info represents any constraints that can be pushed down
    pub fn has_constraints(&self) -> bool {
        self.limit.is_some() || self.offset.is_some()
    }

    /// Apply limit and offset to a vector of data
    pub fn apply_to_vec<T>(&self, mut data: Vec<T>) -> Vec<T> {
        // Apply offset first
        if let Some(offset) = self.offset {
            let offset_usize = offset.max(0) as usize;
            if offset_usize < data.len() {
                data.drain(0..offset_usize);
            } else {
                // Offset is beyond data length, return empty
                return Vec::new();
            }
        }

        // Apply limit
        if let Some(limit) = self.limit {
            let limit_usize = limit.max(0) as usize;
            data.truncate(limit_usize);
        }

        data
    }
}

/// Extract LIMIT and OFFSET information from PostgreSQL planner
/// 
/// # Safety
/// This function assumes valid pointers and proper PostgreSQL context
pub unsafe fn extract_limit_offset_info(
    root: *mut pg_sys::PlannerInfo,
) -> Option<LimitOffsetInfo> {
    if root.is_null() {
        return None;
    }

    let parse = (*root).parse;
    if parse.is_null() {
        return None;
    }

    let mut limit_info = LimitOffsetInfo::new();
    let mut found_any = false;

    // Extract LIMIT count if present
    let limit_count = (*parse).limitCount as *mut pg_sys::Const;
    if !limit_count.is_null() && pgrx::is_a(limit_count as *mut pg_sys::Node, pg_sys::NodeTag::T_Const) {
        if let Some(count) = i64::from_polymorphic_datum(
            (*limit_count).constvalue,
            (*limit_count).constisnull,
            (*limit_count).consttype,
        ) {
            if count > 0 {
                limit_info.limit = Some(count as usize);
                found_any = true;
                log!("Extracted LIMIT: {}", count);
            }
        }
    }

    // Extract OFFSET if present
    let limit_offset = (*parse).limitOffset as *mut pg_sys::Const;
    if !limit_offset.is_null() && pgrx::is_a(limit_offset as *mut pg_sys::Node, pg_sys::NodeTag::T_Const) {
        if let Some(offset) = i64::from_polymorphic_datum(
            (*limit_offset).constvalue,
            (*limit_offset).constisnull,
            (*limit_offset).consttype,
        ) {
            if offset >= 0 {
                limit_info.offset = Some(offset as usize);
                found_any = true;
                log!("Extracted OFFSET: {}", offset);
            }
        }
    }

    if found_any {
        Some(limit_info)
    } else {
        None
    }
}
