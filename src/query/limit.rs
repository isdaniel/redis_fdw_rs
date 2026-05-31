use pgrx::{pg_sys, prelude::*, FromDatum};

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

    /// Check if this info represents any constraints that can be pushed down
    pub fn has_constraints(&self) -> bool {
        self.limit.is_some() || self.offset.is_some()
    }

    /// Apply limit and offset to a vector of data
    pub fn apply_to_vec<T>(&self, mut data: Vec<T>) -> Vec<T> {
        // Apply offset first
        if let Some(offset) = self.offset {
            if offset < data.len() {
                data.drain(0..offset);
            } else {
                return Vec::new();
            }
        }

        // Apply limit
        if let Some(limit) = self.limit {
            data.truncate(limit);
        }

        data
    }
}

/// Extract LIMIT and OFFSET information from PostgreSQL planner
///
/// # Safety
/// This function assumes valid pointers and proper PostgreSQL context
pub unsafe fn extract_limit_offset_info(root: *mut pg_sys::PlannerInfo) -> Option<LimitOffsetInfo> {
    if root.is_null() {
        return None;
    }

    let parse = (*root).parse;
    if parse.is_null() {
        return None;
    }

    // LIMIT/OFFSET cannot be pushed to the scan level when there are row-reducing or reordering operations above. In those cases the LIMIT applies to the output of the aggregate/sort, not the base scan.
    if (*parse).hasAggs
        || (*parse).hasWindowFuncs
        || !(*parse).groupClause.is_null()
        || !(*parse).distinctClause.is_null()
        || !(*parse).sortClause.is_null()
    {
        return None;
    }

    let mut limit_info = LimitOffsetInfo::new();
    let mut found_any = false;

    // Extract LIMIT count if present
    let limit_count = (*parse).limitCount as *mut pg_sys::Const;
    if !limit_count.is_null()
        && pgrx::is_a(limit_count as *mut pg_sys::Node, pg_sys::NodeTag::T_Const)
    {
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
    if !limit_offset.is_null()
        && pgrx::is_a(limit_offset as *mut pg_sys::Node, pg_sys::NodeTag::T_Const)
    {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_limit_offset_new() {
        let info = LimitOffsetInfo::new();
        assert_eq!(info.limit, None);
        assert_eq!(info.offset, None);
        assert!(!info.has_constraints());
    }

    #[test]
    fn test_has_constraints_with_limit() {
        let info = LimitOffsetInfo {
            limit: Some(10),
            offset: None,
        };
        assert!(info.has_constraints());
    }

    #[test]
    fn test_has_constraints_with_offset() {
        let info = LimitOffsetInfo {
            limit: None,
            offset: Some(5),
        };
        assert!(info.has_constraints());
    }

    #[test]
    fn test_apply_to_vec_limit_only() {
        let data = vec![1, 2, 3, 4, 5];
        let info = LimitOffsetInfo {
            limit: Some(3),
            offset: None,
        };
        assert_eq!(info.apply_to_vec(data), vec![1, 2, 3]);
    }

    #[test]
    fn test_apply_to_vec_offset_only() {
        let data = vec![1, 2, 3, 4, 5];
        let info = LimitOffsetInfo {
            limit: None,
            offset: Some(2),
        };
        assert_eq!(info.apply_to_vec(data), vec![3, 4, 5]);
    }

    #[test]
    fn test_apply_to_vec_limit_and_offset() {
        let data = vec![1, 2, 3, 4, 5];
        let info = LimitOffsetInfo {
            limit: Some(2),
            offset: Some(1),
        };
        assert_eq!(info.apply_to_vec(data), vec![2, 3]);
    }

    #[test]
    fn test_apply_to_vec_offset_beyond_length() {
        let data = vec![1, 2, 3];
        let info = LimitOffsetInfo {
            limit: Some(5),
            offset: Some(10),
        };
        assert!(info.apply_to_vec(data).is_empty());
    }

    #[test]
    fn test_apply_to_vec_zero_limit() {
        let data = vec![1, 2, 3];
        let info = LimitOffsetInfo {
            limit: Some(0),
            offset: None,
        };
        assert!(info.apply_to_vec(data).is_empty());
    }

    #[test]
    fn test_apply_to_vec_empty_input() {
        let data: Vec<i32> = vec![];
        let info = LimitOffsetInfo {
            limit: Some(5),
            offset: Some(0),
        };
        assert!(info.apply_to_vec(data).is_empty());
    }

    #[test]
    fn test_apply_to_vec_no_constraints() {
        let data = vec![1, 2, 3, 4, 5];
        let info = LimitOffsetInfo::new();
        assert_eq!(info.apply_to_vec(data.clone()), data);
    }
}
