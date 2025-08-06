#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::{
        query::{
            limit::LimitOffsetInfo,
            pushdown_types::{ComparisonOperator, PushableCondition, PushdownAnalysis},
        },
        tables::{
            implementations::{RedisHashTable, RedisListTable, RedisStringTable},
            interface::RedisTableOperations,
            types::{DataContainer, DataSet},
        },
    };
    use pgrx::prelude::*;

    #[test]
    fn test_limit_offset_info_creation() {
        let limit_info = LimitOffsetInfo::new();
        assert!(!limit_info.has_constraints());

        let limit_info_with_limit = LimitOffsetInfo::with_limit_offset(Some(10), None);
        assert!(limit_info_with_limit.has_constraints());
        assert_eq!(limit_info_with_limit.limit, Some(10));
        assert_eq!(limit_info_with_limit.offset, None);

        let limit_info_with_offset = LimitOffsetInfo::with_limit_offset(Some(5), Some(3));
        assert!(limit_info_with_offset.has_constraints());
        assert_eq!(limit_info_with_offset.limit, Some(5));
        assert_eq!(limit_info_with_offset.offset, Some(3));
    }

    #[test]
    fn test_limit_offset_apply_to_vec() {
        let data = vec![
            "item1".to_string(),
            "item2".to_string(),
            "item3".to_string(),
            "item4".to_string(),
            "item5".to_string(),
        ];

        // Test LIMIT only
        let limit_info = LimitOffsetInfo::with_limit_offset(Some(3), None);
        let result = limit_info.apply_to_vec(data.clone());
        assert_eq!(result.len(), 3);
        assert_eq!(result, vec!["item1", "item2", "item3"]);

        // Test OFFSET only
        let offset_info = LimitOffsetInfo::with_limit_offset(None, Some(2));
        let result = offset_info.apply_to_vec(data.clone());
        assert_eq!(result.len(), 3);
        assert_eq!(result, vec!["item3", "item4", "item5"]);

        // Test LIMIT and OFFSET
        let limit_offset_info = LimitOffsetInfo::with_limit_offset(Some(2), Some(1));
        let result = limit_offset_info.apply_to_vec(data.clone());
        assert_eq!(result.len(), 2);
        assert_eq!(result, vec!["item2", "item3"]);

        // Test OFFSET beyond data length
        let large_offset_info = LimitOffsetInfo::with_limit_offset(Some(5), Some(10));
        let result = large_offset_info.apply_to_vec(data.clone());
        assert!(result.is_empty());

        // Test zero LIMIT
        let zero_limit_info = LimitOffsetInfo::with_limit_offset(Some(0), None);
        let result = zero_limit_info.apply_to_vec(data.clone());
        assert!(result.is_empty());
    }

    #[test]
    fn test_pushdown_analysis_with_limit_offset() {
        let mut analysis = PushdownAnalysis::new();
        assert!(!analysis.has_optimizations());
        assert!(!analysis.has_limit_pushdown());

        // Add LIMIT/OFFSET
        let limit_info = LimitOffsetInfo::with_limit_offset(Some(5), Some(2));
        analysis.set_limit_offset(Some(limit_info));
        assert!(analysis.has_optimizations());
        assert!(analysis.has_limit_pushdown());

        // Add WHERE conditions
        let condition = PushableCondition {
            column_name: "key".to_string(),
            operator: ComparisonOperator::Equal,
            value: "test".to_string(),
        };
        analysis.pushable_conditions.push(condition);
        analysis.can_optimize = true;
        assert!(analysis.has_optimizations());

        // Test with conditions and limit
        let conditions = vec![PushableCondition {
            column_name: "field".to_string(),
            operator: ComparisonOperator::Like,
            value: "pattern%".to_string(),
        }];
        let limit_info = LimitOffsetInfo::with_limit_offset(Some(10), None);
        let combined_analysis =
            PushdownAnalysis::with_conditions_and_limit(conditions, Some(limit_info));
        assert!(combined_analysis.has_optimizations());
        assert!(combined_analysis.has_limit_pushdown());
        assert_eq!(combined_analysis.pushable_conditions.len(), 1);
    }

    #[test]
    fn test_set_filtered_data() {
        let mut string_table = RedisStringTable::new();
        let mut list_table = RedisListTable::new();
        let mut hash_table = RedisHashTable::new();

        let test_data = vec!["data1".to_string(), "data2".to_string()];

        // Test set_filtered_data for all table types
        string_table.set_filtered_data(test_data.clone());
        list_table.set_filtered_data(test_data.clone());
        hash_table.set_filtered_data(test_data.clone());

        match &string_table.dataset {
            DataSet::Filtered(data) => assert_eq!(data, &test_data),
            _ => panic!("Expected Filtered dataset for string table"),
        }

        match &list_table.dataset {
            DataSet::Filtered(data) => assert_eq!(data, &test_data),
            _ => panic!("Expected Filtered dataset for list table"),
        }

        match &hash_table.dataset {
            DataSet::Filtered(data) => assert_eq!(data, &test_data),
            _ => panic!("Expected Filtered dataset for hash table"),
        }
    }

    // PostgreSQL-specific integration tests
    #[test]
    fn pg_test_limit_offset_basic_functionality() {
        // Test basic LIMIT/OFFSET data manipulation
        let data = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ];

        // LIMIT 3
        let limit_info = LimitOffsetInfo::with_limit_offset(Some(3), None);
        let result = limit_info.apply_to_vec(data.clone());
        assert_eq!(result, vec!["a", "b", "c"]);

        // OFFSET 2
        let offset_info = LimitOffsetInfo::with_limit_offset(None, Some(2));
        let result = offset_info.apply_to_vec(data.clone());
        assert_eq!(result, vec!["c", "d", "e"]);

        // LIMIT 2 OFFSET 1
        let combined_info = LimitOffsetInfo::with_limit_offset(Some(2), Some(1));
        let result = combined_info.apply_to_vec(data);
        assert_eq!(result, vec!["b", "c"]);
    }

    #[test]
    fn pg_test_limit_offset_edge_cases() {
        let data = vec!["x".to_string(), "y".to_string()];

        // LIMIT larger than data
        let large_limit = LimitOffsetInfo::with_limit_offset(Some(10), None);
        let result = large_limit.apply_to_vec(data.clone());
        assert_eq!(result, data);

        // OFFSET equal to data length
        let equal_offset = LimitOffsetInfo::with_limit_offset(None, Some(2));
        let result = equal_offset.apply_to_vec(data.clone());
        assert!(result.is_empty());

        // OFFSET larger than data
        let large_offset = LimitOffsetInfo::with_limit_offset(None, Some(5));
        let result = large_offset.apply_to_vec(data.clone());
        assert!(result.is_empty());

        // Zero LIMIT
        let zero_limit = LimitOffsetInfo::with_limit_offset(Some(0), None);
        let result = zero_limit.apply_to_vec(data);
        assert!(result.is_empty());
    }

    #[test]
    fn pg_test_pushdown_analysis_integration() {
        // Test that PushdownAnalysis properly handles LIMIT/OFFSET
        let mut analysis = PushdownAnalysis::new();

        // Initially no optimizations
        assert!(!analysis.has_optimizations());
        assert!(!analysis.has_limit_pushdown());

        // Add LIMIT/OFFSET
        let limit_info = LimitOffsetInfo::with_limit_offset(Some(10), Some(5));
        analysis.set_limit_offset(Some(limit_info));

        // Should now have optimizations
        assert!(analysis.has_optimizations());
        assert!(analysis.has_limit_pushdown());

        // Verify the stored LIMIT/OFFSET info
        assert!(analysis.limit_offset.is_some());
        let stored_info = analysis.limit_offset.as_ref().unwrap();
        assert_eq!(stored_info.limit, Some(10));
        assert_eq!(stored_info.offset, Some(5));
    }
}
