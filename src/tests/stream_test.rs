#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::{
        query::ComparisonOperator,
        tables::{DataSet, RedisStreamTable, RedisTableOperations},
    };

    #[test]
    fn test_stream_table_creation() {
        let table = RedisStreamTable::new(1000);
        assert_eq!(table.batch_size, 1000);
        assert!(table.last_id.is_none());
        assert!(matches!(table.dataset, DataSet::Empty));
    }

    #[test]
    fn test_stream_table_with_batch_size() {
        let table = RedisStreamTable::new(500);
        assert_eq!(table.batch_size, 500);
    }

    #[test]
    fn test_supports_pushdown() {
        let table = RedisStreamTable::new(1000);
        assert!(table.supports_pushdown(&ComparisonOperator::Equal));
        assert!(table.supports_pushdown(&ComparisonOperator::NotEqual));
        assert!(table.supports_pushdown(&ComparisonOperator::Like));
        assert!(!table.supports_pushdown(&ComparisonOperator::In));
        assert!(!table.supports_pushdown(&ComparisonOperator::NotIn));
    }

    // Integration tests with real Redis server
    fn setup_redis_connection() -> redis::Connection {
        let client =
            redis::Client::open("redis://127.0.0.1:8899/").expect("Failed to create Redis client");
        client
            .get_connection()
            .expect("Failed to connect to Redis server")
    }

    fn cleanup_test_stream(conn: &mut redis::Connection, key: &str) {
        let _: Result<(), redis::RedisError> = redis::cmd("DEL").arg(key).query(conn);
    }

    #[test]
    fn test_stream_add_entry_integration() {
        let mut conn = setup_redis_connection();
        let mut table = RedisStreamTable::new(1000);
        let test_key = "test:stream:add_entry";

        // Cleanup any existing test data
        cleanup_test_stream(&mut conn, test_key);

        // Test adding entry with auto-generated ID
        let fields = vec![
            ("user_id".to_string(), "123".to_string()),
            ("action".to_string(), "login".to_string()),
            ("timestamp".to_string(), "2024-01-01T10:00:00Z".to_string()),
        ];

        let result = table.add_entry(&mut conn, test_key, "*", &fields);
        assert!(result.is_ok());

        let stream_id = result.unwrap();
        assert!(!stream_id.is_empty());
        assert!(stream_id.contains('-')); // Redis stream ID format: timestamp-sequence

        // Verify the entry was added to Redis
        let entries: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
            .arg(test_key)
            .arg("-")
            .arg("+")
            .query(&mut conn)
            .expect("Failed to query stream");

        assert_eq!(entries.len(), 1);
        let (id, stream_fields) = &entries[0];
        assert_eq!(id, &stream_id);
        assert_eq!(stream_fields.len(), 3);

        // Verify field values
        let field_map: std::collections::HashMap<String, String> =
            stream_fields.iter().cloned().collect();
        assert_eq!(field_map.get("user_id"), Some(&"123".to_string()));
        assert_eq!(field_map.get("action"), Some(&"login".to_string()));
        assert_eq!(
            field_map.get("timestamp"),
            Some(&"2024-01-01T10:00:00Z".to_string())
        );

        // Cleanup
        cleanup_test_stream(&mut conn, test_key);
    }

    #[test]
    fn test_stream_load_data_integration() {
        let mut conn = setup_redis_connection();
        let mut table = RedisStreamTable::new(1000);
        let test_key = "test:stream:load_data";

        // Cleanup any existing test data
        cleanup_test_stream(&mut conn, test_key);

        // Add test data directly to Redis
        let _: String = redis::cmd("XADD")
            .arg(test_key)
            .arg("*")
            .arg("event_type")
            .arg("user_login")
            .arg("user_id")
            .arg("123")
            .arg("ip")
            .arg("192.168.1.1")
            .query(&mut conn)
            .expect("Failed to add test entry 1");

        let _: String = redis::cmd("XADD")
            .arg(test_key)
            .arg("*")
            .arg("event_type")
            .arg("page_view")
            .arg("user_id")
            .arg("123")
            .arg("page")
            .arg("/dashboard")
            .query(&mut conn)
            .expect("Failed to add test entry 2");

        let _: String = redis::cmd("XADD")
            .arg(test_key)
            .arg("*")
            .arg("event_type")
            .arg("user_logout")
            .arg("user_id")
            .arg("123")
            .arg("session_duration")
            .arg("45m")
            .query(&mut conn)
            .expect("Failed to add test entry 3");

        // Test loading data without conditions
        let result = table.load_data(&mut conn, test_key, None);
        assert!(result.is_ok());

        match result.unwrap() {
            crate::tables::types::LoadDataResult::LoadedToInternal => {
                assert_eq!(table.data_len(), 3);

                // Verify we can get rows
                let row1 = table.get_row(0);
                assert!(row1.is_some());
                let row1_data = row1.unwrap();
                assert!(row1_data.len() >= 4); // stream_id + at least 3 field/value pairs

                // Verify the row contains expected data
                let row1_str = row1_data.join(",");
                assert!(
                    row1_str.contains("user_login")
                        || row1_str.contains("page_view")
                        || row1_str.contains("user_logout")
                );
            }
            _ => panic!("Expected LoadedToInternal result"),
        }

        // Cleanup
        cleanup_test_stream(&mut conn, test_key);
    }

    #[test]
    fn test_stream_large_data_batch_processing() {
        let mut conn = setup_redis_connection();
        let mut table = RedisStreamTable::new(5); // Small batch for testing
        let test_key = "test:stream:batch_processing";

        // Cleanup any existing test data
        cleanup_test_stream(&mut conn, test_key);

        // Add 10 test entries
        for i in 0..10 {
            let _: String = redis::cmd("XADD")
                .arg(test_key)
                .arg("*")
                .arg("event_id")
                .arg(i.to_string())
                .arg("event_type")
                .arg("test_event")
                .arg("data")
                .arg(format!("test_data_{}", i))
                .query(&mut conn)
                .expect(&format!("Failed to add test entry {}", i));
        }

        // Load data with batch processing
        let result = table.load_data(&mut conn, test_key, None);
        assert!(result.is_ok());

        // Should load only the first batch (5 entries due to batch_size)
        match result.unwrap() {
            crate::tables::types::LoadDataResult::LoadedToInternal => {
                assert_eq!(table.data_len(), 5); // Only first batch loaded
                assert!(table.last_id.is_some()); // Should track last ID for pagination
            }
            _ => panic!("Expected LoadedToInternal result"),
        }

        // Test loading next batch
        let next_result = table.load_next_batch(&mut conn, test_key);
        assert!(next_result.is_ok());

        // Cleanup
        cleanup_test_stream(&mut conn, test_key);
    }

    #[test]
    fn test_stream_insert_delete_operations() {
        let mut conn = setup_redis_connection();
        let mut table = RedisStreamTable::new(1000);
        let test_key = "test:stream:insert_delete";

        // Cleanup any existing test data
        cleanup_test_stream(&mut conn, test_key);

        // Test insert operation
        let insert_data = vec![
            "*".to_string(), // Auto-generate ID
            "event_type".to_string(),
            "test_insert".to_string(),
            "user_id".to_string(),
            "999".to_string(),
        ];

        let insert_result = table.insert(&mut conn, test_key, &insert_data);
        assert!(insert_result.is_ok());

        // Verify entry was inserted
        let entries: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
            .arg(test_key)
            .arg("-")
            .arg("+")
            .query(&mut conn)
            .expect("Failed to query stream after insert");

        assert_eq!(entries.len(), 1);
        let (stream_id, _) = &entries[0];

        // Test delete operation
        let delete_data = vec![stream_id.clone()];
        let delete_result = table.delete(&mut conn, test_key, &delete_data);
        assert!(delete_result.is_ok());

        // Verify entry was deleted
        let entries_after_delete: Vec<(String, Vec<(String, String)>)> = redis::cmd("XRANGE")
            .arg(test_key)
            .arg("-")
            .arg("+")
            .query(&mut conn)
            .expect("Failed to query stream after delete");

        assert_eq!(entries_after_delete.len(), 0);

        // Cleanup
        cleanup_test_stream(&mut conn, test_key);
    }

    #[test]
    fn test_stream_range_queries() {
        let mut conn = setup_redis_connection();
        let mut table = RedisStreamTable::new(1000);
        let test_key = "test:stream:range_queries";

        // Cleanup any existing test data
        cleanup_test_stream(&mut conn, test_key);

        // Add entries with specific IDs for range testing
        let _: String = redis::cmd("XADD")
            .arg(test_key)
            .arg("1000000000000-0")
            .arg("event")
            .arg("first")
            .query(&mut conn)
            .expect("Failed to add first entry");

        let _: String = redis::cmd("XADD")
            .arg(test_key)
            .arg("2000000000000-0")
            .arg("event")
            .arg("second")
            .query(&mut conn)
            .expect("Failed to add second entry");

        let _: String = redis::cmd("XADD")
            .arg(test_key)
            .arg("3000000000000-0")
            .arg("event")
            .arg("third")
            .query(&mut conn)
            .expect("Failed to add third entry");

        // Test range query using pushdown conditions
        use crate::query::pushdown_types::{ComparisonOperator, PushableCondition};
        let condition = PushableCondition {
            column_name: "stream_id".to_string(),
            operator: ComparisonOperator::Equal,
            value: "2000000000000-0".to_string(),
        };

        let range_result = table.load_data(&mut conn, test_key, Some(&[condition]));
        assert!(range_result.is_ok());

        // Should load the specific entry
        match range_result.unwrap() {
            crate::tables::types::LoadDataResult::LoadedToInternal => {
                assert_eq!(table.data_len(), 1);
            }
            _ => panic!("Expected LoadedToInternal result"),
        }

        // Cleanup
        cleanup_test_stream(&mut conn, test_key);
    }

    #[test]
    fn test_stream_get_length() {
        let mut conn = setup_redis_connection();
        let table = RedisStreamTable::new(1000);
        let test_key = "test:stream:length";

        // Cleanup any existing test data
        cleanup_test_stream(&mut conn, test_key);

        // Initially should be 0
        let length_result = table.get_stream_length(&mut conn, test_key);
        assert!(length_result.is_ok());
        assert_eq!(length_result.unwrap(), 0);

        // Add some entries
        for i in 0..5 {
            let _: String = redis::cmd("XADD")
                .arg(test_key)
                .arg("*")
                .arg("index")
                .arg(i.to_string())
                .query(&mut conn)
                .expect(&format!("Failed to add entry {}", i));
        }

        // Should now be 5
        let length_after_adds = table.get_stream_length(&mut conn, test_key);
        assert!(length_after_adds.is_ok());
        assert_eq!(length_after_adds.unwrap(), 5);

        // Cleanup
        cleanup_test_stream(&mut conn, test_key);
    }

    #[test]
    fn test_stream_error_handling() {
        let mut conn = setup_redis_connection();
        let mut table = RedisStreamTable::new(1000);

        // Test with non-existent stream
        let result = table.load_data(&mut conn, "non:existent:stream", None);
        assert!(result.is_ok());

        match result.unwrap() {
            crate::tables::types::LoadDataResult::Empty => {
                assert_eq!(table.data_len(), 0);
            }
            _ => panic!("Expected Empty result for non-existent stream"),
        }

        // Test insert with empty data
        let empty_insert = table.insert(&mut conn, "test:empty", &[]);
        assert!(empty_insert.is_ok()); // Should handle gracefully

        // Test delete with empty data
        let empty_delete = table.delete(&mut conn, "test:empty", &[]);
        assert!(empty_delete.is_ok()); // Should handle gracefully
    }
}
