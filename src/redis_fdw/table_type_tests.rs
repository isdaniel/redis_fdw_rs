/// Example tests for the new object-oriented Redis table types
/// These tests demonstrate how each table type can be used independently
#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema] 
mod tests {
    use pgrx::pg_sys;

    use crate::redis_fdw::{
        tables::{RedisTableOperations, RedisHashTable, RedisListTable, RedisSetTable, RedisStringTable, RedisZSetTable}, 
        state::RedisTableType
    };   

    #[test]
    fn test_redis_string_table() {
        let mut string_table = RedisStringTable::new();
        
        // Initially empty
        assert_eq!(string_table.data_len(None), 0);
        assert_eq!(string_table.get_row(0, None), None);
        
        // Simulate setting data
        string_table.data = Some("Hello, World!".to_string());
        
        // Now has data
        assert_eq!(string_table.data_len(None), 1);
        assert_eq!(string_table.get_row(0, None), Some(vec!["Hello, World!".to_string()]));
        assert_eq!(string_table.get_row(1, None), None);
        
        // Test with different values
        string_table.data = Some("Test String".to_string());
        assert_eq!(string_table.get_row(0, None), Some(vec!["Test String".to_string()]));
        
        // Test empty string
        string_table.data = Some("".to_string());
        assert_eq!(string_table.data_len(None), 1);
        assert_eq!(string_table.get_row(0, None), Some(vec!["".to_string()]));
        
        // Test None data
        string_table.data = None;
        assert_eq!(string_table.data_len(None), 0);
        assert_eq!(string_table.get_row(0, None), None);
    }

    #[test]
    fn test_redis_hash_table() {
        let mut hash_table = RedisHashTable::new();
        
        // Initially empty
        assert_eq!(hash_table.data_len(None), 0);
        
        // Add some data
        hash_table.data = vec![
            ("name".to_string(), "John".to_string()),
            ("age".to_string(), "30".to_string()),
            ("city".to_string(), "New York".to_string()),
        ];
        
        // Check data
        assert_eq!(hash_table.data_len(None), 3);
        assert_eq!(hash_table.get_row(0, None), Some(vec!["name".to_string(), "John".to_string()]));
        assert_eq!(hash_table.get_row(1, None), Some(vec!["age".to_string(), "30".to_string()]));
        assert_eq!(hash_table.get_row(2, None), Some(vec!["city".to_string(), "New York".to_string()]));
        assert_eq!(hash_table.get_row(3, None), None);
    }

    #[test]
    fn test_redis_list_table() {
        let mut list_table = RedisListTable::new();
        
        // Initially empty
        assert_eq!(list_table.data_len(None), 0);
        
        // Add some data
        list_table.data = vec![
            "apple".to_string(),
            "banana".to_string(),
            "cherry".to_string(),
        ];
        
        // Check data
        assert_eq!(list_table.data_len(None), 3);
        assert_eq!(list_table.get_row(0, None), Some(vec!["apple".to_string()]));
        assert_eq!(list_table.get_row(1, None), Some(vec!["banana".to_string()]));
        assert_eq!(list_table.get_row(2, None), Some(vec!["cherry".to_string()]));
        assert_eq!(list_table.get_row(3, None), None);
    }

    #[test]
    fn test_redis_set_table() {
        let mut set_table = RedisSetTable::new();
        
        // Initially empty
        assert_eq!(set_table.data_len(None), 0);
        assert_eq!(set_table.get_row(0, None), None);
        
        // Add some data
        set_table.data = vec![
            "red".to_string(),
            "green".to_string(),
            "blue".to_string(),
        ];
        
        // Check data
        assert_eq!(set_table.data_len(None), 3);
        assert_eq!(set_table.get_row(0, None), Some(vec!["red".to_string()]));
        assert_eq!(set_table.get_row(1, None), Some(vec!["green".to_string()]));
        assert_eq!(set_table.get_row(2, None), Some(vec!["blue".to_string()]));
        assert_eq!(set_table.get_row(3, None), None);
        
        // Test with filtered data
        let filtered_data = vec!["yellow".to_string(), "purple".to_string()];
        assert_eq!(set_table.data_len(Some(&filtered_data)), 2);
        assert_eq!(set_table.get_row(0, Some(&filtered_data)), Some(vec!["yellow".to_string()]));
        assert_eq!(set_table.get_row(1, Some(&filtered_data)), Some(vec!["purple".to_string()]));
        assert_eq!(set_table.get_row(2, Some(&filtered_data)), None);
        
        // Test empty set
        set_table.data = vec![];
        assert_eq!(set_table.data_len(None), 0);
        assert_eq!(set_table.get_row(0, None), None);
    }

    #[test]
    fn test_redis_zset_table() {
        let mut zset_table = RedisZSetTable::new();
        
        // Initially empty
        assert_eq!(zset_table.data_len(None), 0);
        assert_eq!(zset_table.get_row(0, None), None);
        
        // Add some data
        zset_table.data = vec![
            ("player1".to_string(), 100.5),
            ("player2".to_string(), 95.0),
            ("player3".to_string(), 110.2),
        ];
        
        // Check data
        assert_eq!(zset_table.data_len(None), 3);
        assert_eq!(zset_table.get_row(0, None), Some(vec!["player1".to_string(), "100.5".to_string()]));
        assert_eq!(zset_table.get_row(1, None), Some(vec!["player2".to_string(), "95".to_string()]));
        assert_eq!(zset_table.get_row(2, None), Some(vec!["player3".to_string(), "110.2".to_string()]));
        assert_eq!(zset_table.get_row(3, None), None);
        
        // Test with filtered data (member-score pairs)
        let filtered_data = vec![
            "player4".to_string(), "120.0".to_string(),
            "player5".to_string(), "130.5".to_string()
        ];
        assert_eq!(zset_table.data_len(Some(&filtered_data)), 2);
        assert_eq!(zset_table.get_row(0, Some(&filtered_data)), Some(vec!["player4".to_string(), "120.0".to_string()]));
        assert_eq!(zset_table.get_row(1, Some(&filtered_data)), Some(vec!["player5".to_string(), "130.5".to_string()]));
        assert_eq!(zset_table.get_row(2, Some(&filtered_data)), None);
        
        // Test edge cases
        zset_table.data = vec![("single".to_string(), 0.0)];
        assert_eq!(zset_table.data_len(None), 1);
        assert_eq!(zset_table.get_row(0, None), Some(vec!["single".to_string(), "0".to_string()]));
        
        // Test empty zset
        zset_table.data = vec![];
        assert_eq!(zset_table.data_len(None), 0);
        assert_eq!(zset_table.get_row(0, None), None);
    }

    #[test]
    fn test_redis_table_type_enum() {
        // Test creation from string
        let string_type = RedisTableType::from_str("string");
        assert!(matches!(string_type, RedisTableType::String(_)));
        
        let hash_type = RedisTableType::from_str("hash");
        assert!(matches!(hash_type, RedisTableType::Hash(_)));
        
        let list_type = RedisTableType::from_str("list");
        assert!(matches!(list_type, RedisTableType::List(_)));
        
        let set_type = RedisTableType::from_str("set");
        assert!(matches!(set_type, RedisTableType::Set(_)));
        
        let zset_type = RedisTableType::from_str("zset");
        assert!(matches!(zset_type, RedisTableType::ZSet(_)));
        
        let none_type = RedisTableType::from_str("invalid");
        assert!(matches!(none_type, RedisTableType::None));
        
        // Test case insensitivity
        let hash_upper = RedisTableType::from_str("HASH");
        assert!(matches!(hash_upper, RedisTableType::Hash(_)));
        
        let list_upper = RedisTableType::from_str("LIST");
        assert!(matches!(list_upper, RedisTableType::List(_)));
        
        let set_upper = RedisTableType::from_str("SET");
        assert!(matches!(set_upper, RedisTableType::Set(_)));
        
        let zset_upper = RedisTableType::from_str("ZSET");
        assert!(matches!(zset_upper, RedisTableType::ZSet(_)));
    }

    #[test]
    fn test_unified_interface() {
        // Test that all table types support the unified interface
        let string_type = RedisTableType::from_str("string");
        let hash_type = RedisTableType::from_str("hash");
        let list_type = RedisTableType::from_str("list");
        let set_type = RedisTableType::from_str("set");
        let zset_type = RedisTableType::from_str("zset");
        
        // All should start with zero length
        assert_eq!(string_type.data_len(None), 0);
        assert_eq!(hash_type.data_len(None), 0);
        assert_eq!(list_type.data_len(None), 0);
        assert_eq!(set_type.data_len(None), 0);
        assert_eq!(zset_type.data_len(None), 0);
        
        // All should return None for get_row on empty data
        assert_eq!(string_type.get_row(0, None), None);
        assert_eq!(hash_type.get_row(0, None), None);
        assert_eq!(list_type.get_row(0, None), None);
        assert_eq!(set_type.get_row(0, None), None);
        assert_eq!(zset_type.get_row(0, None), None);
    }

    #[test]
    fn test_hash_table_advanced_operations() {
        let mut hash_table = RedisHashTable::new();
        
        // Test with various data types as strings
        hash_table.data = vec![
            ("user_id".to_string(), "12345".to_string()),
            ("score".to_string(), "98.5".to_string()),
            ("active".to_string(), "true".to_string()),
            ("name".to_string(), "John O'Connor".to_string()),
            ("description".to_string(), "User with special chars: @#$%^&*()".to_string()),
        ];
        
        assert_eq!(hash_table.data_len(None), 5);
        
        // Test specific field retrieval
        assert_eq!(hash_table.get_row(0, None), Some(vec!["user_id".to_string(), "12345".to_string()]));
        assert_eq!(hash_table.get_row(3, None), Some(vec!["name".to_string(), "John O'Connor".to_string()]));
        
        // Test out of bounds
        assert_eq!(hash_table.get_row(10, None), None);
        
        // Test with unicode fields and values
        hash_table.data = vec![
            ("用户名".to_string(), "张三".to_string()),
            ("🎯".to_string(), "target".to_string()),
        ];
        assert_eq!(hash_table.data_len(None), 2);
        assert_eq!(hash_table.get_row(0, None), Some(vec!["用户名".to_string(), "张三".to_string()]));
        assert_eq!(hash_table.get_row(1, None), Some(vec!["🎯".to_string(), "target".to_string()]));
    }

    #[test]
    fn test_list_table_advanced_operations() {
        let mut list_table = RedisListTable::new();
        
        // Test with mixed content
        list_table.data = vec![
            "first".to_string(),
            "".to_string(),              // empty string
            "123".to_string(),           // numeric string
            "hello world".to_string(),   // space in string
            "special@#$%".to_string(),   // special characters
            "🚀🌟".to_string(),          // unicode emojis
        ];
        
        assert_eq!(list_table.data_len(None), 6);
        assert_eq!(list_table.get_row(0, None), Some(vec!["first".to_string()]));
        assert_eq!(list_table.get_row(1, None), Some(vec!["".to_string()]));
        assert_eq!(list_table.get_row(5, None), Some(vec!["🚀🌟".to_string()]));
        assert_eq!(list_table.get_row(6, None), None);
        
        // Test large list simulation
        let large_list: Vec<String> = (0..1000).map(|i| format!("item_{}", i)).collect();
        list_table.data = large_list;
        assert_eq!(list_table.data_len(None), 1000);
        assert_eq!(list_table.get_row(0, None), Some(vec!["item_0".to_string()]));
        assert_eq!(list_table.get_row(999, None), Some(vec!["item_999".to_string()]));
        assert_eq!(list_table.get_row(1000, None), None);
    }

    #[test]
    fn test_set_table_advanced_operations() {
        let mut set_table = RedisSetTable::new();
        
        // Test with various member types
        set_table.data = vec![
            "user:123".to_string(),
            "tag:urgent".to_string(),
            "category:work".to_string(),
            "status:active".to_string(),
            "123".to_string(),
            "true".to_string(),
            "".to_string(),  // empty string member
        ];
        
        assert_eq!(set_table.data_len(None), 7);
        assert_eq!(set_table.get_row(0, None), Some(vec!["user:123".to_string()]));
        assert_eq!(set_table.get_row(6, None), Some(vec!["".to_string()]));
        
        // Test filtered data with various types
        let filtered = vec![
            "admin".to_string(),
            "moderator".to_string(), 
            "user".to_string(),
            "guest".to_string()
        ];
        assert_eq!(set_table.data_len(Some(&filtered)), 4);
        assert_eq!(set_table.get_row(0, Some(&filtered)), Some(vec!["admin".to_string()]));
        assert_eq!(set_table.get_row(3, Some(&filtered)), Some(vec!["guest".to_string()]));
        assert_eq!(set_table.get_row(4, Some(&filtered)), None);
    }

    #[test]
    fn test_zset_table_advanced_operations() {
        let mut zset_table = RedisZSetTable::new();
        
        // Test with various score scenarios
        zset_table.data = vec![
            ("user1".to_string(), 100.0),
            ("user2".to_string(), -50.5),        // negative score
            ("user3".to_string(), 0.0),          // zero score
            ("user4".to_string(), 999999.99),    // large score
            ("user5".to_string(), 0.001),        // small positive score
            ("特殊用户".to_string(), 88.8),      // unicode member
        ];
        
        assert_eq!(zset_table.data_len(None), 6);
        assert_eq!(zset_table.get_row(0, None), Some(vec!["user1".to_string(), "100".to_string()]));
        assert_eq!(zset_table.get_row(1, None), Some(vec!["user2".to_string(), "-50.5".to_string()]));
        assert_eq!(zset_table.get_row(2, None), Some(vec!["user3".to_string(), "0".to_string()]));
        assert_eq!(zset_table.get_row(5, None), Some(vec!["特殊用户".to_string(), "88.8".to_string()]));
        
        // Test edge case with same member name
        zset_table.data = vec![
            ("duplicate".to_string(), 10.0),
            ("duplicate".to_string(), 20.0),  // Redis would overwrite, but our structure allows it
        ];
        assert_eq!(zset_table.data_len(None), 2);
        assert_eq!(zset_table.get_row(0, None), Some(vec!["duplicate".to_string(), "10".to_string()]));
        assert_eq!(zset_table.get_row(1, None), Some(vec!["duplicate".to_string(), "20".to_string()]));
    }

    #[test]
    fn test_string_table_advanced_operations() {
        let mut string_table = RedisStringTable::new();
        
        // Test with various string types
        let test_strings = vec![
            "simple string",
            "",  // empty string
            "string with spaces and symbols !@#$%^&*()",
            "multi\nline\nstring\nwith\nnewlines",
            "unicode: 你好世界 🌍",
            "very long string that could represent a large text value or JSON document or any other type of data that might be stored in Redis as a string value",
            "123456789",  // numeric string
            "true",       // boolean string
            "null",       // null string
        ];
        
        for test_string in test_strings {
            string_table.data = Some(test_string.to_string());
            assert_eq!(string_table.data_len(None), 1);
            assert_eq!(string_table.get_row(0, None), Some(vec![test_string.to_string()]));
            assert_eq!(string_table.get_row(1, None), None);
        }
        
        // Test with filtered data
        let filtered = vec!["filtered_value".to_string()];
        string_table.data = Some("original_value".to_string());
        assert_eq!(string_table.data_len(Some(&filtered)), 1);
        assert_eq!(string_table.get_row(0, Some(&filtered)), Some(vec!["filtered_value".to_string()]));
    }

    #[test]
    fn test_all_table_types_consistency() {
        // Test that all table types behave consistently with edge cases
        let hash_table = RedisHashTable::new();
        let list_table = RedisListTable::new();
        let set_table = RedisSetTable::new();
        let zset_table = RedisZSetTable::new();
        let string_table = RedisStringTable::new();
        
        // All empty tables should have zero length
        assert_eq!(hash_table.data_len(None), 0);
        assert_eq!(list_table.data_len(None), 0);
        assert_eq!(set_table.data_len(None), 0);
        assert_eq!(zset_table.data_len(None), 0);
        assert_eq!(string_table.data_len(None), 0);
        
        // All empty tables should return None for any row access
        for i in 0..5 {
            assert_eq!(hash_table.get_row(i, None), None);
            assert_eq!(list_table.get_row(i, None), None);
            assert_eq!(set_table.get_row(i, None), None);
            assert_eq!(zset_table.get_row(i, None), None);
            assert_eq!(string_table.get_row(i, None), None);
        }
        
        // Test with empty filtered data
        let empty_filtered: Vec<String> = vec![];
        assert_eq!(hash_table.data_len(Some(&empty_filtered)), 0);
        assert_eq!(list_table.data_len(Some(&empty_filtered)), 0);
        assert_eq!(set_table.data_len(Some(&empty_filtered)), 0);
        assert_eq!(zset_table.data_len(Some(&empty_filtered)), 0);
        assert_eq!(string_table.data_len(Some(&empty_filtered)), 0);
    }

    #[test]
    fn test_hash_table_edge_cases() {
        let mut hash_table = RedisHashTable::new();
        
        // Test with empty data
        assert_eq!(hash_table.data_len(None), 0);
        assert_eq!(hash_table.get_row(0, None), None);
        
        // Test with single entry
        hash_table.data = vec![("key1".to_string(), "value1".to_string())];
        assert_eq!(hash_table.data_len(None), 1);
        assert_eq!(hash_table.get_row(0, None), Some(vec!["key1".to_string(), "value1".to_string()]));
        
        // Test with empty strings
        hash_table.data = vec![("".to_string(), "".to_string())];
        assert_eq!(hash_table.data_len(None), 1);
        assert_eq!(hash_table.get_row(0, None), Some(vec!["".to_string(), "".to_string()]));
        
        // Test with special characters
        hash_table.data = vec![("key@#$".to_string(), "value!@#$%^&*()".to_string())];
        assert_eq!(hash_table.get_row(0, None), Some(vec!["key@#$".to_string(), "value!@#$%^&*()".to_string()]));
    }

    #[test]
    fn test_list_table_edge_cases() {
        let mut list_table = RedisListTable::new();
        
        // Test with single element
        list_table.data = vec!["single".to_string()];
        assert_eq!(list_table.data_len(None), 1);
        assert_eq!(list_table.get_row(0, None), Some(vec!["single".to_string()]));
        
        // Test with empty strings in list
        list_table.data = vec!["".to_string(), "non-empty".to_string(), "".to_string()];
        assert_eq!(list_table.data_len(None), 3);
        assert_eq!(list_table.get_row(0, None), Some(vec!["".to_string()]));
        assert_eq!(list_table.get_row(1, None), Some(vec!["non-empty".to_string()]));
        assert_eq!(list_table.get_row(2, None), Some(vec!["".to_string()]));
        
        // Test with unicode characters
        list_table.data = vec!["🚀".to_string(), "测试".to_string(), "עברית".to_string()];
        assert_eq!(list_table.data_len(None), 3);
        assert_eq!(list_table.get_row(0, None), Some(vec!["🚀".to_string()]));
        assert_eq!(list_table.get_row(1, None), Some(vec!["测试".to_string()]));
        assert_eq!(list_table.get_row(2, None), Some(vec!["עברית".to_string()]));
    }

    #[test]
    fn test_set_table_edge_cases() {
        let mut set_table = RedisSetTable::new();
        
        // Test with duplicate-like values (sets should handle uniqueness in Redis, but we test the structure)
        set_table.data = vec!["value".to_string(), "value".to_string()];
        assert_eq!(set_table.data_len(None), 2); // Our structure doesn't enforce uniqueness, Redis does
        assert_eq!(set_table.get_row(0, None), Some(vec!["value".to_string()]));
        assert_eq!(set_table.get_row(1, None), Some(vec!["value".to_string()]));
        
        // Test with numeric strings
        set_table.data = vec!["1".to_string(), "2.5".to_string(), "-10".to_string()];
        assert_eq!(set_table.data_len(None), 3);
        assert_eq!(set_table.get_row(0, None), Some(vec!["1".to_string()]));
        assert_eq!(set_table.get_row(1, None), Some(vec!["2.5".to_string()]));
        assert_eq!(set_table.get_row(2, None), Some(vec!["-10".to_string()]));
    }

    #[test]
    fn test_zset_table_edge_cases() {
        let mut zset_table = RedisZSetTable::new();
        
        // Test with same scores
        zset_table.data = vec![
            ("member1".to_string(), 100.0),
            ("member2".to_string(), 100.0),
        ];
        assert_eq!(zset_table.data_len(None), 2);
        assert_eq!(zset_table.get_row(0, None), Some(vec!["member1".to_string(), "100".to_string()]));
        assert_eq!(zset_table.get_row(1, None), Some(vec!["member2".to_string(), "100".to_string()]));
        
        // Test with negative scores
        zset_table.data = vec![
            ("negative".to_string(), -50.5),
            ("positive".to_string(), 50.5),
        ];
        assert_eq!(zset_table.get_row(0, None), Some(vec!["negative".to_string(), "-50.5".to_string()]));
        assert_eq!(zset_table.get_row(1, None), Some(vec!["positive".to_string(), "50.5".to_string()]));
        
        // Test with zero score
        zset_table.data = vec![("zero".to_string(), 0.0)];
        assert_eq!(zset_table.get_row(0, None), Some(vec!["zero".to_string(), "0".to_string()]));
        
        // Test with very large numbers
        zset_table.data = vec![("large".to_string(), f64::MAX)];
        assert_eq!(zset_table.get_row(0, None), Some(vec!["large".to_string(), f64::MAX.to_string()]));
    }

    #[test]
    fn test_filtered_data_functionality() {
        // Test that all table types handle filtered data correctly
        let hash_table = RedisHashTable::new();
        let list_table = RedisListTable::new();
        let set_table = RedisSetTable::new();
        let zset_table = RedisZSetTable::new();
        let string_table = RedisStringTable::new();
        
        // Test with filtered data for hash (field-value pairs)
        let hash_filtered = vec!["field1".to_string(), "value1".to_string(), "field2".to_string(), "value2".to_string()];
        assert_eq!(hash_table.data_len(Some(&hash_filtered)), 2);
        assert_eq!(hash_table.get_row(0, Some(&hash_filtered)), Some(vec!["field1".to_string(), "value1".to_string()]));
        assert_eq!(hash_table.get_row(1, Some(&hash_filtered)), Some(vec!["field2".to_string(), "value2".to_string()]));
        
        // Test with filtered data for list
        let list_filtered = vec!["item1".to_string(), "item2".to_string(), "item3".to_string()];
        assert_eq!(list_table.data_len(Some(&list_filtered)), 3);
        assert_eq!(list_table.get_row(0, Some(&list_filtered)), Some(vec!["item1".to_string()]));
        assert_eq!(list_table.get_row(2, Some(&list_filtered)), Some(vec!["item3".to_string()]));
        
        // Test with filtered data for set
        let set_filtered = vec!["member1".to_string(), "member2".to_string()];
        assert_eq!(set_table.data_len(Some(&set_filtered)), 2);
        assert_eq!(set_table.get_row(0, Some(&set_filtered)), Some(vec!["member1".to_string()]));
        
        // Test with filtered data for zset (member-score pairs)
        let zset_filtered = vec!["player1".to_string(), "100.0".to_string(), "player2".to_string(), "200.0".to_string()];
        assert_eq!(zset_table.data_len(Some(&zset_filtered)), 2);
        assert_eq!(zset_table.get_row(0, Some(&zset_filtered)), Some(vec!["player1".to_string(), "100.0".to_string()]));
        
        // Test with filtered data for string (single value)
        let string_filtered = vec!["filtered_value".to_string()];
        assert_eq!(string_table.data_len(Some(&string_filtered)), 1);
        assert_eq!(string_table.get_row(0, Some(&string_filtered)), Some(vec!["filtered_value".to_string()]));
    }

}
