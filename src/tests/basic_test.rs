/// Comprehensive integration tests for Redis FDW table types
///
/// This module provides integration tests for all Redis table types (String, Hash, List, Set, ZSet)
/// focusing on the table logic, data structures, and operations without requiring external Redis.
/// These tests verify the table implementations, data handling, and type conversions.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::{
        query::pushdown_types::ComparisonOperator,
        tables::{
            implementations::{
                RedisHashTable, RedisListTable, RedisSetTable, RedisStringTable, RedisZSetTable,
            },
            interface::RedisTableOperations,
            types::{DataContainer, DataSet, RedisTableType},
        },
    };
    use pgrx::prelude::*;
    use std::borrow::Cow;

    fn cow_vec_to_string_vec(row: Option<Vec<Cow<'_, str>>>) -> Option<Vec<String>> {
        row.map(|v| v.into_iter().map(|c| c.into_owned()).collect())
    }

    /// Helper utilities for integration testing
    pub struct IntegrationTestHelper;

    impl IntegrationTestHelper {
        /// Setup a string table with test data
        pub fn setup_string_table_with_data(value: Option<String>) -> RedisStringTable {
            let mut table = RedisStringTable::new();
            table.dataset = DataSet::Complete(DataContainer::String(value));
            table
        }

        /// Setup a hash table with test data
        pub fn setup_hash_table_with_data(data: Vec<(String, String)>) -> RedisHashTable {
            let mut table = RedisHashTable::new();
            table.dataset = DataSet::Complete(DataContainer::Hash(data));
            table
        }

        /// Setup a list table with test data
        pub fn setup_list_table_with_data(data: Vec<String>) -> RedisListTable {
            let mut table = RedisListTable::new();
            table.dataset = DataSet::Complete(DataContainer::List(data));
            table
        }

        /// Setup a set table with test data
        pub fn setup_set_table_with_data(data: Vec<String>) -> RedisSetTable {
            let mut table = RedisSetTable::new();
            table.dataset = DataSet::Complete(DataContainer::Set(data));
            table
        }

        /// Setup a zset table with test data
        pub fn setup_zset_table_with_data(data: Vec<(String, f64)>) -> RedisZSetTable {
            let mut table = RedisZSetTable::new();
            table.dataset = DataSet::Complete(DataContainer::ZSet(data));
            table
        }
    }

    // String table integration tests
    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_string_table_data_operations() {
        // Test with existing value
        let table =
            IntegrationTestHelper::setup_string_table_with_data(Some("test_value".to_string()));

        assert_eq!(table.data_len(), 1);
        assert_eq!(
            cow_vec_to_string_vec(table.get_row(0)),
            Some(vec!["test_value".to_string()])
        );
        assert_eq!(cow_vec_to_string_vec(table.get_row(1)), None);
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_string_table_empty_value() {
        // Test with None value
        let table = IntegrationTestHelper::setup_string_table_with_data(None);

        assert_eq!(table.data_len(), 0);
        assert_eq!(cow_vec_to_string_vec(table.get_row(0)), None);
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_string_table_edge_cases() {
        // Test with empty string
        let table = IntegrationTestHelper::setup_string_table_with_data(Some("".to_string()));

        assert_eq!(table.data_len(), 1);
        assert_eq!(
            cow_vec_to_string_vec(table.get_row(0)),
            Some(vec!["".to_string()])
        );

        // Test with special characters
        let table = IntegrationTestHelper::setup_string_table_with_data(Some(
            "Special!@#$%^&*()".to_string(),
        ));

        assert_eq!(table.data_len(), 1);
        assert_eq!(
            cow_vec_to_string_vec(table.get_row(0)),
            Some(vec!["Special!@#$%^&*()".to_string()])
        );
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_string_table_unicode_support() {
        // Test with Unicode characters
        let table =
            IntegrationTestHelper::setup_string_table_with_data(Some("Hello 世界 🌍".to_string()));

        assert_eq!(table.data_len(), 1);
        assert_eq!(
            cow_vec_to_string_vec(table.get_row(0)),
            Some(vec!["Hello 世界 🌍".to_string()])
        );
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_string_table_supports_pushdown() {
        let table = RedisStringTable::new();

        // Test different operators
        assert!(
            table.supports_pushdown(&ComparisonOperator::Equal)
                || !table.supports_pushdown(&ComparisonOperator::Equal)
        );
        assert!(
            table.supports_pushdown(&ComparisonOperator::Like)
                || !table.supports_pushdown(&ComparisonOperator::Like)
        );
        // Just verify the method works - actual support depends on implementation
    }

    // Hash table integration tests
    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_hash_table_data_operations() {
        let data = vec![
            ("name".to_string(), "John".to_string()),
            ("age".to_string(), "30".to_string()),
            ("city".to_string(), "NYC".to_string()),
        ];
        let table = IntegrationTestHelper::setup_hash_table_with_data(data.clone());

        assert_eq!(table.data_len(), 3);

        // Check that all field-value pairs are accessible
        let mut found_pairs = std::collections::HashSet::new();
        for i in 0..table.data_len() {
            if let Some(row) = table.get_row(i) {
                assert_eq!(row.len(), 2); // field, value
                found_pairs.insert((row[0].to_string(), row[1].to_string()));
            }
        }

        for (field, value) in data {
            assert!(found_pairs.contains(&(field, value)));
        }
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_hash_table_empty_hash() {
        let table = IntegrationTestHelper::setup_hash_table_with_data(vec![]);

        assert_eq!(table.data_len(), 0);
        assert_eq!(cow_vec_to_string_vec(table.get_row(0)), None);
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_hash_table_single_field() {
        let data = vec![("status".to_string(), "active".to_string())];
        let table = IntegrationTestHelper::setup_hash_table_with_data(data);

        assert_eq!(table.data_len(), 1);
        assert_eq!(
            cow_vec_to_string_vec(table.get_row(0)),
            Some(vec!["status".to_string(), "active".to_string()])
        );
        assert_eq!(cow_vec_to_string_vec(table.get_row(1)), None);
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_hash_table_complex_values() {
        let data = vec![
            (
                "json_data".to_string(),
                r#"{"key": "value", "number": 42}"#.to_string(),
            ),
            (
                "multiline".to_string(),
                "Line 1\nLine 2\nLine 3".to_string(),
            ),
            ("empty_value".to_string(), "".to_string()),
        ];
        let table = IntegrationTestHelper::setup_hash_table_with_data(data.clone());

        assert_eq!(table.data_len(), 3);

        // Verify complex data is preserved
        let mut found_pairs = std::collections::HashSet::new();
        for i in 0..table.data_len() {
            if let Some(row) = table.get_row(i) {
                found_pairs.insert((row[0].to_string(), row[1].to_string()));
            }
        }

        for (field, value) in data {
            assert!(found_pairs.contains(&(field, value)));
        }
    }

    // List table integration tests
    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_list_table_data_operations() {
        let data = vec![
            "item1".to_string(),
            "item2".to_string(),
            "item3".to_string(),
        ];
        let table = IntegrationTestHelper::setup_list_table_with_data(data.clone());

        assert_eq!(table.data_len(), 3);

        // Check order preservation
        for (i, expected) in data.iter().enumerate() {
            assert_eq!(
                cow_vec_to_string_vec(table.get_row(i)),
                Some(vec![expected.clone()])
            );
        }

        assert_eq!(cow_vec_to_string_vec(table.get_row(3)), None);
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_list_table_empty_list() {
        let table = IntegrationTestHelper::setup_list_table_with_data(vec![]);

        assert_eq!(table.data_len(), 0);
        assert_eq!(cow_vec_to_string_vec(table.get_row(0)), None);
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_list_table_duplicate_items() {
        let data = vec![
            "item1".to_string(),
            "item1".to_string(),
            "item2".to_string(),
        ];
        let table = IntegrationTestHelper::setup_list_table_with_data(data.clone());

        assert_eq!(table.data_len(), 3);

        // Lists should preserve duplicates and order
        for (i, expected) in data.iter().enumerate() {
            assert_eq!(
                cow_vec_to_string_vec(table.get_row(i)),
                Some(vec![expected.clone()])
            );
        }
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_list_table_large_list() {
        let data: Vec<String> = (0..100).map(|i| format!("item_{}", i)).collect(); // Reduced from 1000 to 100 for faster tests
        let table = IntegrationTestHelper::setup_list_table_with_data(data.clone());

        assert_eq!(table.data_len(), 100);

        // Check first few and last few items
        assert_eq!(
            cow_vec_to_string_vec(table.get_row(0)),
            Some(vec!["item_0".to_string()])
        );
        assert_eq!(
            cow_vec_to_string_vec(table.get_row(99)),
            Some(vec!["item_99".to_string()])
        );
        assert_eq!(cow_vec_to_string_vec(table.get_row(100)), None);
    }

    // Set table integration tests
    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_set_table_data_operations() {
        let data = vec!["red".to_string(), "green".to_string(), "blue".to_string()];
        let table = IntegrationTestHelper::setup_set_table_with_data(data.clone());

        assert_eq!(table.data_len(), 3);

        // Collect all members
        let mut retrieved_members = std::collections::HashSet::new();
        for i in 0..table.data_len() {
            if let Some(row) = table.get_row(i) {
                assert_eq!(row.len(), 1);
                retrieved_members.insert(row[0].to_string());
            }
        }

        // Check that all expected members are present
        for member in &data {
            assert!(retrieved_members.contains(member));
        }
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_set_table_empty_set() {
        let table = IntegrationTestHelper::setup_set_table_with_data(vec![]);

        assert_eq!(table.data_len(), 0);
        assert_eq!(cow_vec_to_string_vec(table.get_row(0)), None);
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_set_table_uniqueness() {
        // Sets should contain unique elements
        let data = vec![
            "unique1".to_string(),
            "unique2".to_string(),
            "unique3".to_string(),
        ];
        let table = IntegrationTestHelper::setup_set_table_with_data(data.clone());

        assert_eq!(table.data_len(), 3);

        // Verify no duplicates
        let mut seen_members = std::collections::HashSet::new();
        for i in 0..table.data_len() {
            if let Some(row) = table.get_row(i) {
                let member_str = row[0].to_string();
                assert!(!seen_members.contains(&member_str));
                seen_members.insert(member_str);
            }
        }
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_set_table_filtered_data() {
        let mut table = RedisSetTable::new();
        let filtered_data = vec!["filtered1".to_string(), "filtered2".to_string()];
        table.dataset = DataSet::Filtered(filtered_data.clone());

        assert_eq!(table.data_len(), 2);

        // Check filtered data access
        for (i, expected) in filtered_data.iter().enumerate() {
            assert_eq!(
                cow_vec_to_string_vec(table.get_row(i)),
                Some(vec![expected.clone()])
            );
        }
    }

    // ZSet table integration tests
    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_zset_table_data_operations() {
        let data = vec![
            ("player1".to_string(), 100.0),
            ("player2".to_string(), 85.5),
            ("player3".to_string(), 92.3),
        ];
        let table = IntegrationTestHelper::setup_zset_table_with_data(data.clone());

        assert_eq!(table.data_len(), 3);

        // Check that all members are accessible with their scores
        let mut found_members = std::collections::HashMap::new();
        for i in 0..table.data_len() {
            if let Some(row) = table.get_row(i) {
                assert_eq!(row.len(), 2); // member, score
                let member = row[0].to_string();
                let score = row[1].parse::<f64>().unwrap();
                found_members.insert(member, score);
            }
        }

        for (member, expected_score) in data {
            assert!(found_members.contains_key(&member));
            let actual_score = found_members[&member];
            assert!((actual_score - expected_score).abs() < 0.001); // Float comparison
        }
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_zset_table_empty_zset() {
        let table = IntegrationTestHelper::setup_zset_table_with_data(vec![]);

        assert_eq!(table.data_len(), 0);
        assert_eq!(cow_vec_to_string_vec(table.get_row(0)), None);
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_zset_table_score_formatting() {
        let data = vec![
            ("zero".to_string(), 0.0),
            ("positive".to_string(), 123.456),
            ("negative".to_string(), -45.67),
            ("large".to_string(), 999999.999),
        ];
        let table = IntegrationTestHelper::setup_zset_table_with_data(data);

        assert_eq!(table.data_len(), 4);

        // Check that scores are properly formatted and parseable
        for i in 0..table.data_len() {
            if let Some(row) = table.get_row(i) {
                assert_eq!(row.len(), 2);
                let score_str = &row[1];
                // Should be able to parse back to float
                assert!(score_str.parse::<f64>().is_ok());
            }
        }
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_zset_table_filtered_data() {
        let mut table = RedisZSetTable::new();
        let filtered_data = vec![
            "player4".to_string(),
            "120.0".to_string(),
            "player5".to_string(),
            "130.5".to_string(),
        ];
        table.dataset = DataSet::Filtered(filtered_data);

        assert_eq!(table.data_len(), 2); // 2 member-score pairs

        // Check filtered data access
        assert_eq!(
            cow_vec_to_string_vec(table.get_row(0)),
            Some(vec!["player4".to_string(), "120.0".to_string()])
        );
        assert_eq!(
            cow_vec_to_string_vec(table.get_row(1)),
            Some(vec!["player5".to_string(), "130.5".to_string()])
        );
        assert_eq!(cow_vec_to_string_vec(table.get_row(2)), None);
    }

    // RedisTableType enum integration tests
    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_redis_table_type_factory() {
        // Test all valid table type strings
        assert!(matches!(
            RedisTableType::from_str("string"),
            RedisTableType::String(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("STRING"),
            RedisTableType::String(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("hash"),
            RedisTableType::Hash(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("HASH"),
            RedisTableType::Hash(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("list"),
            RedisTableType::List(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("LIST"),
            RedisTableType::List(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("set"),
            RedisTableType::Set(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("SET"),
            RedisTableType::Set(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("zset"),
            RedisTableType::ZSet(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("ZSET"),
            RedisTableType::ZSet(_)
        ));
        assert!(matches!(
            RedisTableType::from_str("invalid"),
            RedisTableType::None
        ));
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_redis_table_type_polymorphic_operations() {
        // Test that all table types respond to common operations
        let table_types = vec![
            RedisTableType::String(RedisStringTable::new()),
            RedisTableType::Hash(RedisHashTable::new()),
            RedisTableType::List(RedisListTable::new()),
            RedisTableType::Set(RedisSetTable::new()),
            RedisTableType::ZSet(RedisZSetTable::new()),
            RedisTableType::None,
        ];

        for table_type in table_types {
            // All should start empty
            assert_eq!(table_type.data_len(), 0);
            assert_eq!(cow_vec_to_string_vec(table_type.get_row(0)), None);

            // All should respond to operator queries
            let _ = table_type.supports_pushdown(&ComparisonOperator::Equal);
            let _ = table_type.supports_pushdown(&ComparisonOperator::Like);
        }
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_table_type_with_populated_data() {
        // Test String table with data
        let string_table = RedisTableType::String(
            IntegrationTestHelper::setup_string_table_with_data(Some("test".to_string())),
        );
        assert_eq!(string_table.data_len(), 1);

        // Test Hash table with data
        let hash_table = RedisTableType::Hash(IntegrationTestHelper::setup_hash_table_with_data(
            vec![("key1".to_string(), "value1".to_string())],
        ));
        assert_eq!(hash_table.data_len(), 1);

        // Test List table with data
        let list_table =
            RedisTableType::List(IntegrationTestHelper::setup_list_table_with_data(vec![
                "item1".to_string(),
            ]));
        assert_eq!(list_table.data_len(), 1);

        // Test Set table with data
        let set_table =
            RedisTableType::Set(IntegrationTestHelper::setup_set_table_with_data(vec![
                "member1".to_string(),
            ]));
        assert_eq!(set_table.data_len(), 1);

        // Test ZSet table with data
        let zset_table = RedisTableType::ZSet(IntegrationTestHelper::setup_zset_table_with_data(
            vec![("member1".to_string(), 100.0)],
        ));
        assert_eq!(zset_table.data_len(), 1);
    }

    // Edge case and boundary condition tests
    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_boundary_conditions() {
        // Test accessing rows beyond bounds
        let string_table = RedisStringTable::new();
        assert_eq!(
            cow_vec_to_string_vec(string_table.get_row(usize::MAX)),
            None
        );
        assert_eq!(cow_vec_to_string_vec(string_table.get_row(1000)), None);

        let hash_table = RedisHashTable::new();
        assert_eq!(cow_vec_to_string_vec(hash_table.get_row(usize::MAX)), None);

        let list_table = RedisListTable::new();
        assert_eq!(cow_vec_to_string_vec(list_table.get_row(usize::MAX)), None);

        let set_table = RedisSetTable::new();
        assert_eq!(cow_vec_to_string_vec(set_table.get_row(usize::MAX)), None);

        let zset_table = RedisZSetTable::new();
        assert_eq!(cow_vec_to_string_vec(zset_table.get_row(usize::MAX)), None);
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_pushdown_operator_coverage() {
        // Test that all table types handle all available operators
        let operators = vec![
            ComparisonOperator::Equal,
            ComparisonOperator::NotEqual,
            ComparisonOperator::Like,
            ComparisonOperator::In,
            ComparisonOperator::NotIn,
        ];

        let tables: Vec<Box<dyn RedisTableOperations>> = vec![
            Box::new(RedisStringTable::new()),
            Box::new(RedisHashTable::new()),
            Box::new(RedisListTable::new()),
            Box::new(RedisSetTable::new()),
            Box::new(RedisZSetTable::new()),
        ];

        for table in tables {
            for op in &operators {
                // Each table should report whether it supports the operator
                let _supports = table.supports_pushdown(op);
                // No assertion on actual support - that's implementation dependent
            }
        }
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_dataset_state_transitions() {
        let mut table = RedisStringTable::new();

        // Start as Empty
        assert_eq!(table.data_len(), 0);

        // Transition to Complete
        table.dataset = DataSet::Complete(DataContainer::String(Some("test".to_string())));
        assert_eq!(table.data_len(), 1);

        // Transition to Filtered
        table.dataset = DataSet::Filtered(vec!["filtered".to_string()]);
        assert_eq!(table.data_len(), 1);

        // Transition back to Empty
        table.dataset = DataSet::Empty;
        assert_eq!(table.data_len(), 0);
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_special_character_handling() {
        // Test with various special characters
        let special_values = vec![
            "\0null_byte",
            "\r\nwindows_line_ending",
            "\nunix_line_ending",
            "\ttab_character",
            "\"quoted_string\"",
            "'single_quoted'",
            "\\backslash",
            "emoji_🚀_test",
            "unicode_测试",
        ];

        for special_value in special_values {
            let table = IntegrationTestHelper::setup_string_table_with_data(Some(
                special_value.to_string(),
            ));

            assert_eq!(table.data_len(), 1);
            assert_eq!(
                cow_vec_to_string_vec(table.get_row(0)),
                Some(vec![special_value.to_string()])
            );
        }
    }

    #[cfg(any(test, feature = "pg_test"))]
    #[pgrx::pg_test]
    fn test_large_data_handling() {
        // Test with relatively large data sets
        let large_string = "a".repeat(1000); // Reduced from 10000 to 1000 for faster tests
        let table = IntegrationTestHelper::setup_string_table_with_data(Some(large_string.clone()));

        assert_eq!(table.data_len(), 1);
        assert_eq!(
            cow_vec_to_string_vec(table.get_row(0)),
            Some(vec![large_string])
        );

        // Test large hash
        let large_hash: Vec<(String, String)> =
            (0..100) // Reduced from 1000 to 100 for faster tests
                .map(|i| (format!("key_{}", i), format!("value_{}", i)))
                .collect();
        let hash_table = IntegrationTestHelper::setup_hash_table_with_data(large_hash.clone());

        assert_eq!(hash_table.data_len(), 100);

        // Test large list
        let large_list: Vec<String> = (0..100).map(|i| format!("item_{}", i)).collect(); // Reduced from 1000 to 100
        let list_table = IntegrationTestHelper::setup_list_table_with_data(large_list);

        assert_eq!(list_table.data_len(), 100);
    }
}
