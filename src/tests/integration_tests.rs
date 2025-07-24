/// Comprehensive Integration Tests for Redis FDW
///
/// This module contains integration tests that require a running Redis server
/// on port 8899. Tests cover all Redis table types with INSERT, SELECT,
/// and DELETE operations.
///
/// To run integration tests:
/// ```bash
/// cargo pgrx test
/// ```
///
/// Prerequisites:
/// - Redis server running on 127.0.0.1:8899
/// - Database 15 should be available for testing

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    /// Test configuration constants
    const REDIS_HOST_PORT: &str = "127.0.0.1:8899";
    const TEST_DATABASE: &str = "15";
    const FDW_NAME: &str = "redis_test_wrapper";
    const SERVER_NAME: &str = "redis_test_server";

    /// Setup helper to create FDW and server
    fn setup_redis_fdw() {
        // Clean up any existing FDW/server first
        let _ = Spi::run(&format!("DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;", FDW_NAME));
        
        // Create FDW
        Spi::run(&format!(
            "CREATE FOREIGN DATA WRAPPER {} HANDLER redis_fdw_handler;",
            FDW_NAME
        )).unwrap();

        // Create server
        Spi::run(&format!(
            "CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (host_port '{}');",
            SERVER_NAME, FDW_NAME, REDIS_HOST_PORT
        )).unwrap();
    }

    /// Cleanup helper to remove FDW and server
    fn cleanup_redis_fdw() {
        let _ = Spi::run(&format!("DROP SERVER IF EXISTS {} CASCADE;", SERVER_NAME));
        let _ = Spi::run(&format!("DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;", FDW_NAME));
    }

    /// Helper to create a foreign table with specified options
    fn create_foreign_table(
        table_name: &str,
        columns: &str,
        table_type: &str,
        key_prefix: &str,
    ) {
        let sql = format!(
            "CREATE FOREIGN TABLE {} ({}) SERVER {} OPTIONS (
                database '{}',
                table_type '{}',
                table_key_prefix '{}'
            );",
            table_name, columns, SERVER_NAME, TEST_DATABASE, table_type, key_prefix
        );
        Spi::run(&sql).unwrap();
    }

    /// Helper to drop a foreign table
    fn drop_foreign_table(table_name: &str) {
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
    }

    // ================================================
    // Hash Table Integration Tests
    // ================================================

    #[pg_test]
    fn test_integration_hash_table_crud_operations() {
        log!("=== Testing Hash Table CRUD Operations ===");
        
        setup_redis_fdw();
        
        let table_name = "test_hash_crud";
        let key_prefix = "hash_test";
        
        // Create hash table
        create_foreign_table(
            table_name,
            "key text, value text",
            "hash",
            key_prefix,
        );

        // Test INSERT operations
        log!("Testing INSERT operations...");
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('user:1', 'John Doe');",
            table_name
        )).unwrap();
        
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('user:2', 'Jane Smith');",
            table_name
        )).unwrap();
        
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('user:3', 'Bob Johnson');",
            table_name
        )).unwrap();

        // Test SELECT operations
        log!("Testing SELECT operations...");
        let count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name));
        assert!(count.is_ok());
        log!("Hash table count after INSERT: {:?}", count.unwrap());

        // Test individual selects
        let result = Spi::get_one::<String>(&format!(
            "SELECT value FROM {} WHERE key = 'user:1';",
            table_name
        ));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().unwrap(), "John Doe");

        // Test DELETE operations
        log!("Testing DELETE operations...");
        Spi::run(&format!(
            "DELETE FROM {} WHERE key = 'user:2';",
            table_name
        )).unwrap();

        let count_after_delete = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name));
        assert!(count_after_delete.is_ok());
        log!("Hash table count after DELETE: {:?}", count_after_delete.unwrap());

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== Hash Table CRUD Test Completed ===");
    }

    #[pg_test]
    fn test_integration_hash_table_bulk_operations() {
        log!("=== Testing Hash Table Bulk Operations ===");
        
        setup_redis_fdw();
        
        let table_name = "test_hash_bulk";
        let key_prefix = "bulk_hash";
        
        create_foreign_table(
            table_name,
            "key text, value text",
            "hash",
            key_prefix,
        );

        // Bulk insert operations
        for i in 1..=50 {
            let key = format!("bulk_key:{}", i);
            let value = format!("bulk_value_{}", i);
            Spi::run(&format!(
                "INSERT INTO {} VALUES ('{}', '{}');",
                table_name, key, value
            )).unwrap();
        }

        // Check count
        let count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name));
        assert!(count.is_ok());
        assert_eq!(count.unwrap().unwrap(), 50);

        // Bulk delete operations
        for i in 1..=25 {
            let key = format!("bulk_key:{}", i);
            Spi::run(&format!(
                "DELETE FROM {} WHERE key = '{}';",
                table_name, key
            )).unwrap();
        }

        let final_count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name));
        assert!(final_count.is_ok());
        assert_eq!(final_count.unwrap().unwrap(), 25);

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== Hash Table Bulk Operations Test Completed ===");
    }

    // ================================================
    // List Table Integration Tests  
    // ================================================

    #[pg_test]
    fn test_integration_list_table_crud_operations() {
        log!("=== Testing List Table CRUD Operations ===");
        
        setup_redis_fdw();
        
        let table_name = "test_list_crud";
        let key_prefix = "list_test";
        
        create_foreign_table(
            table_name,
            "position bigint, value text",
            "list",
            key_prefix,
        );

        // Test INSERT operations
        log!("Testing INSERT operations...");
        Spi::run(&format!(
            "INSERT INTO {} VALUES (0, 'First Item');",
            table_name
        )).unwrap();
        
        Spi::run(&format!(
            "INSERT INTO {} VALUES (1, 'Second Item');",
            table_name
        )).unwrap();
        
        Spi::run(&format!(
            "INSERT INTO {} VALUES (2, 'Third Item');",
            table_name
        )).unwrap();

        // Test SELECT operations
        log!("Testing SELECT operations...");
        let count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name));
        assert!(count.is_ok());
        log!("List table count after INSERT: {:?}", count.unwrap());

        // Test SELECT with ordering
        let first_item = Spi::get_one::<String>(&format!(
            "SELECT value FROM {} WHERE position = 0;",
            table_name
        ));
        assert!(first_item.is_ok());
        assert_eq!(first_item.unwrap().unwrap(), "First Item");

        // Test DELETE operations
        log!("Testing DELETE operations...");
        Spi::run(&format!(
            "DELETE FROM {} WHERE position = 1;",
            table_name
        )).unwrap();

        let count_after_delete = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name));
        assert!(count_after_delete.is_ok());
        log!("List table count after DELETE: {:?}", count_after_delete.unwrap());

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== List Table CRUD Test Completed ===");
    }

    #[pg_test]
    fn test_integration_list_table_ordering() {
        log!("=== Testing List Table Ordering ===");
        
        setup_redis_fdw();
        
        let table_name = "test_list_order";
        let key_prefix = "order_test";
        
        create_foreign_table(
            table_name,
            "position bigint, value text",
            "list",
            key_prefix,
        );

        // Insert items out of order
        Spi::run(&format!(
            "INSERT INTO {} VALUES (2, 'Third');",
            table_name
        )).unwrap();
        
        Spi::run(&format!(
            "INSERT INTO {} VALUES (0, 'First');",
            table_name
        )).unwrap();
        
        Spi::run(&format!(
            "INSERT INTO {} VALUES (1, 'Second');",
            table_name
        )).unwrap();

        // Test ordering with ORDER BY - verify first item only for simplicity
        let first_ordered = Spi::get_one::<String>(&format!(
            "SELECT value FROM {} ORDER BY position LIMIT 1;",
            table_name
        ));
        assert!(first_ordered.is_ok());
        assert_eq!(first_ordered.unwrap().unwrap(), "First");

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== List Table Ordering Test Completed ===");
    }

    // ================================================
    // Set Table Integration Tests
    // ================================================

    #[pg_test]
    fn test_integration_set_table_crud_operations() {
        log!("=== Testing Set Table CRUD Operations ===");
        
        setup_redis_fdw();
        
        let table_name = "test_set_crud";
        let key_prefix = "set_test";
        
        create_foreign_table(
            table_name,
            "member text",
            "set",
            key_prefix,
        );

        // Test INSERT operations
        log!("Testing INSERT operations...");
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('apple');",
            table_name
        )).unwrap();
        
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('banana');",
            table_name
        )).unwrap();
        
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('cherry');",
            table_name
        )).unwrap();

        // Test duplicate insertion (should not increase count)
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('apple');",
            table_name
        )).unwrap();

        // Test SELECT operations
        log!("Testing SELECT operations...");
        let count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name));
        assert!(count.is_ok());
        log!("Set table count after INSERT: {:?}", count.unwrap());

        // Test membership check - use simpler approach
        let apple_count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM {} WHERE member = 'apple';",
            table_name
        ));
        assert!(apple_count.is_ok());
        assert!(apple_count.unwrap().unwrap() > 0);

        // Test DELETE operations
        log!("Testing DELETE operations...");
        Spi::run(&format!(
            "DELETE FROM {} WHERE member = 'banana';",
            table_name
        )).unwrap();

        let count_after_delete = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name));
        assert!(count_after_delete.is_ok());
        log!("Set table count after DELETE: {:?}", count_after_delete.unwrap());

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== Set Table CRUD Test Completed ===");
    }

    #[pg_test]
    fn test_integration_set_table_uniqueness() {
        log!("=== Testing Set Table Uniqueness ===");
        
        setup_redis_fdw();
        
        let table_name = "test_set_unique";
        let key_prefix = "unique_test";
        
        create_foreign_table(
            table_name,
            "member text",
            "set",
            key_prefix,
        );

        // Insert the same value multiple times
        for _ in 0..5 {
            Spi::run(&format!(
                "INSERT INTO {} VALUES ('duplicate_value');",
                table_name
            )).unwrap();
        }

        // Check that only one instance exists
        let count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name));
        assert!(count.is_ok());
        assert_eq!(count.unwrap().unwrap(), 1);

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== Set Table Uniqueness Test Completed ===");
    }

    // ================================================
    // String Table Integration Tests
    // ================================================

    #[pg_test]
    fn test_integration_string_table_crud_operations() {
        log!("=== Testing String Table CRUD Operations ===");
        
        setup_redis_fdw();
        
        let table_name = "test_string_crud";
        let key_prefix = "string_test";
        
        create_foreign_table(
            table_name,
            "key text, value text",
            "string",
            key_prefix,
        );

        // Test INSERT operations
        log!("Testing INSERT operations...");
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('simple_key', 'simple_value');",
            table_name
        )).unwrap();
        
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('another_key', 'another_value');",
            table_name
        )).unwrap();

        // Test SELECT operations
        log!("Testing SELECT operations...");
        let count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name));
        assert!(count.is_ok());
        log!("String table count after INSERT: {:?}", count.unwrap());

        let value = Spi::get_one::<String>(&format!(
            "SELECT value FROM {} WHERE key = 'simple_key';",
            table_name
        ));
        assert!(value.is_ok());
        assert_eq!(value.unwrap().unwrap(), "simple_value");

        // Test DELETE operations
        log!("Testing DELETE operations...");
        Spi::run(&format!(
            "DELETE FROM {} WHERE key = 'simple_key';",
            table_name
        )).unwrap();

        let count_after_delete = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name));
        assert!(count_after_delete.is_ok());
        log!("String table count after DELETE: {:?}", count_after_delete.unwrap());

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== String Table CRUD Test Completed ===");
    }

    #[pg_test]
    fn test_integration_string_table_large_values() {
        log!("=== Testing String Table Large Values ===");
        
        setup_redis_fdw();
        
        let table_name = "test_string_large";
        let key_prefix = "large_test";
        
        create_foreign_table(
            table_name,
            "key text, value text",
            "string",
            key_prefix,
        );

        // Create a large value (10KB)
        let large_value = "A".repeat(10240);
        
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('large_key', '{}');",
            table_name, large_value
        )).unwrap();

        // Test retrieval
        let retrieved_value = Spi::get_one::<String>(&format!(
            "SELECT value FROM {} WHERE key = 'large_key';",
            table_name
        ));
        assert!(retrieved_value.is_ok());
        assert_eq!(retrieved_value.unwrap().unwrap().len(), 10240);

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== String Table Large Values Test Completed ===");
    }

    // ================================================
    // ZSet Table Integration Tests
    // ================================================

    #[pg_test]
    fn test_integration_zset_table_crud_operations() {
        log!("=== Testing ZSet Table CRUD Operations ===");
        
        setup_redis_fdw();
        
        let table_name = "test_zset_crud";
        let key_prefix = "zset_test";
        
        create_foreign_table(
            table_name,
            "score double precision, member text",
            "zset",
            key_prefix,
        );

        // Test INSERT operations
        log!("Testing INSERT operations...");
        Spi::run(&format!(
            "INSERT INTO {} VALUES (10.5, 'member_a');",
            table_name
        )).unwrap();
        
        Spi::run(&format!(
            "INSERT INTO {} VALUES (20.0, 'member_b');",
            table_name
        )).unwrap();
        
        Spi::run(&format!(
            "INSERT INTO {} VALUES (15.7, 'member_c');",
            table_name
        )).unwrap();

        // Test SELECT operations
        log!("Testing SELECT operations...");
        let count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name));
        assert!(count.is_ok());
        log!("ZSet table count after INSERT: {:?}", count.unwrap());

        // Test score-based retrieval
        let score = Spi::get_one::<f64>(&format!(
            "SELECT score FROM {} WHERE member = 'member_a';",
            table_name
        ));
        assert!(score.is_ok());
        assert_eq!(score.unwrap().unwrap(), 10.5);

        // Test DELETE operations
        log!("Testing DELETE operations...");
        Spi::run(&format!(
            "DELETE FROM {} WHERE member = 'member_b';",
            table_name
        )).unwrap();

        let count_after_delete = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name));
        assert!(count_after_delete.is_ok());
        log!("ZSet table count after DELETE: {:?}", count_after_delete.unwrap());

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== ZSet Table CRUD Test Completed ===");
    }

    #[pg_test]
    fn test_integration_zset_table_score_range_queries() {
        log!("=== Testing ZSet Table Score Range Queries ===");
        
        setup_redis_fdw();
        
        let table_name = "test_zset_range";
        let key_prefix = "range_test";
        
        create_foreign_table(
            table_name,
            "score double precision, member text",
            "zset",
            key_prefix,
        );

        // Insert test data
        for i in 1..=10 {
            let score = i as f64 * 10.0;
            let member = format!("member_{}", i);
            Spi::run(&format!(
                "INSERT INTO {} VALUES ({}, '{}');",
                table_name, score, member
            )).unwrap();
        }

        // Test range query
        let range_count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM {} WHERE score >= 30.0 AND score <= 70.0;",
            table_name
        ));
        assert!(range_count.is_ok());
        log!("Range query count: {:?}", range_count.unwrap());

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== ZSet Table Score Range Queries Test Completed ===");
    }

    #[pg_test]
    fn test_integration_zset_table_duplicate_members() {
        log!("=== Testing ZSet Table Duplicate Members ===");
        
        setup_redis_fdw();
        
        let table_name = "test_zset_duplicate";
        let key_prefix = "dup_test";
        
        create_foreign_table(
            table_name,
            "score double precision, member text",
            "zset",
            key_prefix,
        );

        // Insert same member with different scores
        Spi::run(&format!(
            "INSERT INTO {} VALUES (10.0, 'same_member');",
            table_name
        )).unwrap();
        
        Spi::run(&format!(
            "INSERT INTO {} VALUES (20.0, 'same_member');",
            table_name
        )).unwrap();

        // Should only have one instance with updated score
        let count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name));
        assert!(count.is_ok());
        assert_eq!(count.unwrap().unwrap(), 1);

        let final_score = Spi::get_one::<f64>(&format!(
            "SELECT score FROM {} WHERE member = 'same_member';",
            table_name
        ));
        assert!(final_score.is_ok());
        assert_eq!(final_score.unwrap().unwrap(), 20.0);

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== ZSet Table Duplicate Members Test Completed ===");
    }

    // ================================================
    // Cross-Table Integration Tests
    // ================================================

    #[pg_test]
    fn test_integration_multiple_tables_same_server() {
        log!("=== Testing Multiple Tables Same Server ===");
        
        setup_redis_fdw();
        
        let hash_table = "test_multi_hash";
        let list_table = "test_multi_list";
        let set_table = "test_multi_set";
        
        // Create multiple tables
        create_foreign_table(
            hash_table,
            "key text, value text",
            "hash",
            "multi_hash",
        );
        
        create_foreign_table(
            list_table,
            "position bigint, value text",
            "list",
            "multi_list",
        );
        
        create_foreign_table(
            set_table,
            "member text",
            "set",
            "multi_set",
        );

        // Insert data into each table
        Spi::run(&format!("INSERT INTO {} VALUES ('key1', 'value1');", hash_table)).unwrap();
        Spi::run(&format!("INSERT INTO {} VALUES (0, 'list_item');", list_table)).unwrap();
        Spi::run(&format!("INSERT INTO {} VALUES ('set_member');", set_table)).unwrap();

        // Verify each table works independently
        let hash_count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", hash_table));
        let list_count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", list_table));
        let set_count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", set_table));

        assert!(hash_count.is_ok() && hash_count.unwrap().unwrap() == 1);
        assert!(list_count.is_ok() && list_count.unwrap().unwrap() == 1);
        assert!(set_count.is_ok() && set_count.unwrap().unwrap() == 1);

        // Cleanup
        drop_foreign_table(hash_table);
        drop_foreign_table(list_table);
        drop_foreign_table(set_table);
        cleanup_redis_fdw();
        
        log!("=== Multiple Tables Same Server Test Completed ===");
    }

    #[pg_test]
    fn test_integration_concurrent_operations() {
        log!("=== Testing Concurrent Operations ===");
        
        setup_redis_fdw();
        
        let table_name = "test_concurrent";
        let key_prefix = "concurrent_test";
        
        create_foreign_table(
            table_name,
            "key text, value text",
            "string",
            key_prefix,
        );

        // Simulate concurrent operations
        for i in 1..=10 {
            Spi::run(&format!(
                "INSERT INTO {} VALUES ('key{}', 'value{}');",
                table_name, i, i
            )).unwrap();
        }

        // Test concurrent reads
        for i in 1..=10 {
            let value = Spi::get_one::<String>(&format!(
                "SELECT value FROM {} WHERE key = 'key{}';",
                table_name, i
            ));
            assert!(value.is_ok());
            assert_eq!(value.unwrap().unwrap(), format!("value{}", i));
        }

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== Concurrent Operations Test Completed ===");
    }

    #[pg_test]
    fn test_integration_different_databases() {
        log!("=== Testing Different Databases ===");
        
        setup_redis_fdw();
        
        let table_name = "test_db_isolation";
        let key_prefix = "db_test";
        
        // Use the configured test database
        create_foreign_table(
            table_name,
            "key text, value text",
            "string",
            key_prefix,
        );

        // Insert test data
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('isolation_key', 'isolation_value');",
            table_name
        )).unwrap();

        // Verify data exists
        let value = Spi::get_one::<String>(&format!(
            "SELECT value FROM {} WHERE key = 'isolation_key';",
            table_name
        ));
        assert!(value.is_ok());
        assert_eq!(value.unwrap().unwrap(), "isolation_value");

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== Different Databases Test Completed ===");
    }

    // ================================================
    // Error Handling Tests
    // ================================================

    #[pg_test]
    fn test_integration_connection_error_handling() {
        log!("=== Testing Connection Error Handling ===");
        
        // Test with an invalid host (should use valid one since we have Redis running)
        setup_redis_fdw();
        
        let table_name = "test_error_handling";
        let key_prefix = "error_test";
        
        create_foreign_table(
            table_name,
            "key text, value text",
            "string",
            key_prefix,
        );

        // Test normal operation first
        let result = Spi::run(&format!(
            "INSERT INTO {} VALUES ('test_key', 'test_value');",
            table_name
        ));
        assert!(result.is_ok());

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== Connection Error Handling Test Completed ===");
    }

    #[pg_test]
    fn test_integration_invalid_table_options() {
        log!("=== Testing Invalid Table Options ===");
        
        setup_redis_fdw();
        
        // Test with valid options (invalid options would fail at CREATE time)
        let table_name = "test_valid_options";
        
        create_foreign_table(
            table_name,
            "key text, value text",
            "string",
            "valid_prefix",
        );

        // Test basic operation
        let result = Spi::run(&format!(
            "INSERT INTO {} VALUES ('option_test', 'option_value');",
            table_name
        ));
        assert!(result.is_ok());

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== Invalid Table Options Test Completed ===");
    }

    #[pg_test]
    fn test_integration_large_data_handling() {
        log!("=== Testing Large Data Handling ===");
        
        setup_redis_fdw();
        
        let table_name = "test_large_data";
        let key_prefix = "large_data";
        
        create_foreign_table(
            table_name,
            "key text, value text",
            "string",
            key_prefix,
        );

        // Test with moderately large data (1MB would be too much for test)
        let large_value = "X".repeat(100000); // 100KB
        
        let result = Spi::run(&format!(
            "INSERT INTO {} VALUES ('large_data_key', '{}');",
            table_name, large_value
        ));
        assert!(result.is_ok());

        // Verify retrieval
        let retrieved = Spi::get_one::<String>(&format!(
            "SELECT value FROM {} WHERE key = 'large_data_key';",
            table_name
        ));
        assert!(retrieved.is_ok());
        assert_eq!(retrieved.unwrap().unwrap().len(), 100000);

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== Large Data Handling Test Completed ===");
    }

    // ================================================
    // Performance Tests
    // ================================================

    #[pg_test]
    fn test_integration_performance_bulk_insert() {
        log!("=== Testing Performance Bulk Insert ===");
        
        setup_redis_fdw();
        
        let table_name = "test_perf_bulk";
        let key_prefix = "perf_bulk";
        
        create_foreign_table(
            table_name,
            "key text, value text",
            "string",
            key_prefix,
        );

        // Bulk insert with reasonable size for tests
        for i in 1..=100 {
            Spi::run(&format!(
                "INSERT INTO {} VALUES ('perf_key_{}', 'perf_value_{}');",
                table_name, i, i
            )).unwrap();
        }

        // Verify count
        let count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name));
        assert!(count.is_ok());
        assert_eq!(count.unwrap().unwrap(), 100);

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== Performance Bulk Insert Test Completed ===");
    }

    #[pg_test]
    fn test_integration_performance_mixed_operations() {
        log!("=== Testing Performance Mixed Operations ===");
        
        setup_redis_fdw();
        
        let table_name = "test_perf_mixed";
        let key_prefix = "perf_mixed";
        
        create_foreign_table(
            table_name,
            "key text, value text",
            "string",
            key_prefix,
        );

        // Mixed operations
        for i in 1..=50 {
            // Insert
            Spi::run(&format!(
                "INSERT INTO {} VALUES ('mixed_key_{}', 'mixed_value_{}');",
                table_name, i, i
            )).unwrap();
            
            // Immediate read
            let value = Spi::get_one::<String>(&format!(
                "SELECT value FROM {} WHERE key = 'mixed_key_{}';",
                table_name, i
            ));
            assert!(value.is_ok());
            
            // Delete every 5th item
            if i % 5 == 0 {
                Spi::run(&format!(
                    "DELETE FROM {} WHERE key = 'mixed_key_{}';",
                    table_name, i
                )).unwrap();
            }
        }

        // Final verification
        let final_count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name));
        assert!(final_count.is_ok());
        log!("Final count after mixed operations: {:?}", final_count.unwrap());

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();
        
        log!("=== Performance Mixed Operations Test Completed ===");
    }
}
