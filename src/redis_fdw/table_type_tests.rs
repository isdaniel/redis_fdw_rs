/// Example tests for the new object-oriented Redis table types
/// These tests demonstrate how each table type can be used independently

#[cfg(test)]
mod tests {
    use crate::redis_fdw::{interface::RedisTableOperations, redis_hash_table::RedisHashTable, redis_list_table::RedisListTable, state::*};

    // #[test]
    // fn test_redis_string_table() {
    //     let mut string_table = RedisStringTable::new();
        
    //     // Initially empty
    //     assert_eq!(string_table.data_len(), 0);
    //     assert_eq!(string_table.get_row(0), None);
        
    //     // Simulate setting data
    //     string_table.data = Some("Hello, World!".to_string());
        
    //     // Now has data
    //     assert_eq!(string_table.data_len(), 1);
    //     assert_eq!(string_table.get_row(0), Some(vec!["Hello, World!".to_string()]));
    //     assert_eq!(string_table.get_row(1), None);
    // }

    #[test]
    fn test_redis_hash_table() {
        let mut hash_table = RedisHashTable::new();
        
        // Initially empty
        assert_eq!(hash_table.data_len(), 0);
        
        // Add some data
        hash_table.data = vec![
            ("name".to_string(), "John".to_string()),
            ("age".to_string(), "30".to_string()),
            ("city".to_string(), "New York".to_string()),
        ];
        
        // Check data
        assert_eq!(hash_table.data_len(), 3);
        assert_eq!(hash_table.get_row(0), Some(vec!["name".to_string(), "John".to_string()]));
        assert_eq!(hash_table.get_row(1), Some(vec!["age".to_string(), "30".to_string()]));
        assert_eq!(hash_table.get_row(2), Some(vec!["city".to_string(), "New York".to_string()]));
        assert_eq!(hash_table.get_row(3), None);
    }

    #[test]
    fn test_redis_list_table() {
        let mut list_table = RedisListTable::new();
        
        // Initially empty
        assert_eq!(list_table.data_len(), 0);
        
        // Add some data
        list_table.data = vec![
            "apple".to_string(),
            "banana".to_string(),
            "cherry".to_string(),
        ];
        
        // Check data
        assert_eq!(list_table.data_len(), 3);
        assert_eq!(list_table.get_row(0), Some(vec!["apple".to_string()]));
        assert_eq!(list_table.get_row(1), Some(vec!["banana".to_string()]));
        assert_eq!(list_table.get_row(2), Some(vec!["cherry".to_string()]));
        assert_eq!(list_table.get_row(3), None);
    }

    // #[test]
    // fn test_redis_set_table() {
    //     let mut set_table = RedisSetTable::new();
        
    //     // Initially empty
    //     assert_eq!(set_table.data_len(), 0);
        
    //     // Add some data
    //     set_table.data = vec![
    //         "red".to_string(),
    //         "green".to_string(),
    //         "blue".to_string(),
    //     ];
        
    //     // Check data
    //     assert_eq!(set_table.data_len(), 3);
    //     assert_eq!(set_table.get_row(0), Some(vec!["red".to_string()]));
    //     assert_eq!(set_table.get_row(1), Some(vec!["green".to_string()]));
    //     assert_eq!(set_table.get_row(2), Some(vec!["blue".to_string()]));
    //     assert_eq!(set_table.get_row(3), None);
    // }

    // #[test]
    // fn test_redis_zset_table() {
    //     let mut zset_table = RedisZSetTable::new();
        
    //     // Initially empty
    //     assert_eq!(zset_table.data_len(), 0);
        
    //     // Add some data
    //     zset_table.data = vec![
    //         ("player1".to_string(), 100.5),
    //         ("player2".to_string(), 95.0),
    //         ("player3".to_string(), 110.2),
    //     ];
        
    //     // Check data
    //     assert_eq!(zset_table.data_len(), 3);
    //     assert_eq!(zset_table.get_row(0), Some(vec!["player1".to_string(), "100.5".to_string()]));
    //     assert_eq!(zset_table.get_row(1), Some(vec!["player2".to_string(), "95".to_string()]));
    //     assert_eq!(zset_table.get_row(2), Some(vec!["player3".to_string(), "110.2".to_string()]));
    //     assert_eq!(zset_table.get_row(3), None);
    // }

    #[test]
    fn test_redis_table_type_enum() {
        // Test creation from string
        // let string_type = RedisTableType::from_str("string");
        // assert!(matches!(string_type, RedisTableType::String(_)));
        
        let hash_type = RedisTableType::from_str("hash");
        assert!(matches!(hash_type, RedisTableType::Hash(_)));
        
        let list_type = RedisTableType::from_str("list");
        assert!(matches!(list_type, RedisTableType::List(_)));
        
        // let set_type = RedisTableType::from_str("set");
        // assert!(matches!(set_type, RedisTableType::Set(_)));
        
        // let zset_type = RedisTableType::from_str("zset");
        // assert!(matches!(zset_type, RedisTableType::ZSet(_)));
        
        let none_type = RedisTableType::from_str("invalid");
        assert!(matches!(none_type, RedisTableType::None));
        
        // Test case insensitivity
        let hash_upper = RedisTableType::from_str("HASH");
        assert!(matches!(hash_upper, RedisTableType::Hash(_)));
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
        assert_eq!(string_type.data_len(), 0);
        assert_eq!(hash_type.data_len(), 0);
        assert_eq!(list_type.data_len(), 0);
        assert_eq!(set_type.data_len(), 0);
        assert_eq!(zset_type.data_len(), 0);
        
        // All should return None for get_row on empty data
        assert_eq!(string_type.get_row(0), None);
        assert_eq!(hash_type.get_row(0), None);
        assert_eq!(list_type.get_row(0), None);
        assert_eq!(set_type.get_row(0), None);
        assert_eq!(zset_type.get_row(0), None);
    }
}
