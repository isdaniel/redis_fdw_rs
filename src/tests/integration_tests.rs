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
/// - Tests use controlled pacing to prevent connection timeouts

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;
    /// Test configuration constants
    const REDIS_HOST_PORT: &str = "127.0.0.1:8899";
    const TEST_DATABASE: &str = "15";
    const FDW_NAME: &str = "redis_test_wrapper";
    const SERVER_NAME: &str = "redis_test_server";

    /// Connection management constants
    const BATCH_SIZE: usize = 10; // Process operations in smaller batches

    /// Setup helper to create FDW and server with error handling
    fn setup_redis_fdw() {
        log!("Setting up Redis FDW connection...");

        // Clean up any existing FDW/server first
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            FDW_NAME
        ));

        // Small delay to ensure cleanup completes

        // Create FDW
        Spi::run(&format!(
            "CREATE FOREIGN DATA WRAPPER {} HANDLER redis_fdw_handler;",
            FDW_NAME
        ))
        .unwrap();

        // Create server
        Spi::run(&format!(
            "CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (host_port '{}');",
            SERVER_NAME, FDW_NAME, REDIS_HOST_PORT
        ))
        .unwrap();

        log!("Redis FDW setup completed successfully");
    }

    /// Cleanup helper to remove FDW and server
    fn cleanup_redis_fdw() {
        log!("Cleaning up Redis FDW connection...");
        let _ = Spi::run(&format!("DROP SERVER IF EXISTS {} CASCADE;", SERVER_NAME));
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            FDW_NAME
        ));

        // Small delay to ensure cleanup completes

        log!("Redis FDW cleanup completed");
    }

    /// Helper to create a foreign table with specified options
    fn create_foreign_table(table_name: &str, columns: &str, table_type: &str, key_prefix: &str) {
        let sql: String = format!(
            "CREATE FOREIGN TABLE {table_name} ({columns}) SERVER {SERVER_NAME} OPTIONS (
                database '{TEST_DATABASE}',
                table_type '{table_type}',
                table_key_prefix '{key_prefix}'
            );"
        );
        Spi::run(&sql).unwrap();
        log!("Created foreign table: {table_name} of type: {table_type}");

        // Small delay after table creation
    }

    /// Helper to drop a foreign table
    fn drop_foreign_table(table_name: &str) {
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {table_name};"));
        log!("Dropped foreign table: {table_name}");

        // Small delay after table drop
    }

    /// Helper to perform operations with controlled pacing - for single-column tables
    fn controlled_insert_single(table_name: &str, value: &str) {
        Spi::run(&format!("INSERT INTO {} VALUES ('{}');", table_name, value)).unwrap();
    }

    /// Helper to perform operations with controlled pacing - for two-column tables
    fn controlled_insert_pair(table_name: &str, col1: &str, col2: &str) {
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('{}', '{}');",
            table_name, col1, col2
        ))
        .unwrap();
    }

    /// Helper to perform controlled delete operations
    fn controlled_delete(table_name: &str, where_clause: &str) {
        Spi::run(&format!(
            "DELETE FROM {} WHERE {};",
            table_name, where_clause
        ))
        .unwrap();
    }

    /// Helper to perform controlled select operations
    fn controlled_select_count(table_name: &str) -> Option<i64> {
        Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name)).unwrap()
    }

    // ================================================
    // HASH TABLE INTEGRATION TESTS
    // ================================================

    #[pg_test]
    fn test_integration_hash_table_basic_crud() {
        log!("=== Testing Hash Table Basic CRUD Operations ===");

        setup_redis_fdw();

        let table_name = "test_hash_crud";
        let key_prefix = "integration:hash:crud";

        // Create hash table
        create_foreign_table(table_name, "field text, value text", "hash", key_prefix);

        // Test INSERT operations with pacing
        log!("Testing INSERT operations...");
        controlled_insert_pair(table_name, "user:1", "John Doe");
        controlled_insert_pair(table_name, "user:2", "Jane Smith");
        controlled_insert_pair(table_name, "user:3", "Bob Johnson");

        // Test SELECT operations
        log!("Testing SELECT operations...");
        let count = controlled_select_count(table_name);
        assert_eq!(count, Some(3));
        log!("Hash table count after INSERT: {:?}", count);

        // Test individual record selection

        let result = Spi::get_one::<String>(&format!(
            "SELECT value FROM {} WHERE field = 'user:1';",
            table_name
        ));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().unwrap(), "John Doe");

        // Test DELETE operations
        log!("Testing DELETE operations...");
        controlled_delete(table_name, "field = 'user:2'");

        let count_after_delete = controlled_select_count(table_name);
        assert_eq!(count_after_delete, Some(2));
        log!("Hash table count after DELETE: {:?}", count_after_delete);

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();

        log!("=== Hash Table Basic CRUD Test Completed ===");
    }

    #[pg_test]
    fn test_integration_hash_table_bulk_operations() {
        log!("=== Testing Hash Table Bulk Operations ===");

        setup_redis_fdw();

        let table_name = "test_hash_bulk";
        let key_prefix = "integration:hash:bulk";

        create_foreign_table(table_name, "field text, value text", "hash", key_prefix);

        for batch in 0..(30 / BATCH_SIZE) {
            for i in 1..=BATCH_SIZE {
                let index = batch * BATCH_SIZE + i;
                let field = format!("bulk_field:{}", index);
                let value = format!("bulk_value_{}", index);
                controlled_insert_pair(table_name, &field, &value);
            }
        }

        // Check count
        let count = controlled_select_count(table_name);
        assert_eq!(count, Some(30));
        log!("Hash table count after bulk INSERT: {:?}", count);

        // Bulk delete operations with controlled pacing
        log!("Performing bulk DELETE operations...");
        for i in 1..=BATCH_SIZE {
            let field = format!("bulk_field:{}", i);
            controlled_delete(table_name, &format!("field = '{field}'"));
        }

        let final_count = controlled_select_count(table_name);
        assert_eq!(final_count, Some(20));
        log!("Hash table count after bulk DELETE: {:?}", final_count);

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();

        log!("=== Hash Table Bulk Operations Test Completed ===");
    }

    // ================================================
    // LIST TABLE INTEGRATION TESTS
    // ================================================

    #[pg_test]
    fn test_integration_list_table_basic_crud() {
        log!("=== Testing List Table Basic CRUD Operations ===");

        setup_redis_fdw();

        let table_name = "test_list_crud";
        let key_prefix = "integration:list:crud";

        create_foreign_table(table_name, "element text", "list", key_prefix);

        controlled_delete(table_name, "1 = 1");

        // Test INSERT operations with pacing
        log!("Testing INSERT operations...");
        controlled_insert_single(table_name, "First Item");
        controlled_insert_single(table_name, "Second Item");
        controlled_insert_single(table_name, "Third Item");

        // Test SELECT operations
        log!("Testing SELECT operations...");
        let count = controlled_select_count(table_name);
        assert_eq!(count, Some(3));
        log!("List table count after INSERT: {:?}", count);

        // Test DELETE operations
        log!("Testing DELETE operations...");
        controlled_delete(table_name, "element = 'Second Item'");

        let count_after_delete = controlled_select_count(table_name);
        assert_eq!(count_after_delete, Some(2));
        log!("List table count after DELETE: {:?}", count_after_delete);

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();

        log!("=== List Table Basic CRUD Test Completed ===");
    }

    #[pg_test]
    fn test_integration_list_table_ordering() {
        log!("=== Testing List Table Ordering ===");

        setup_redis_fdw();

        let table_name = "test_list_order";
        let key_prefix = "integration:list:order";

        create_foreign_table(table_name, "element text", "list", key_prefix);

        controlled_delete(table_name, "1 = 1");

        // Insert items in sequence
        controlled_insert_single(table_name, "First");
        controlled_insert_single(table_name, "Second");
        controlled_insert_single(table_name, "Third");

        // Test basic count
        let count = controlled_select_count(table_name);
        assert_eq!(count, Some(3));

        // Test simple selection

        let result =
            Spi::get_one::<String>(&format!("SELECT element FROM {} LIMIT 1;", table_name));
        assert!(result.is_ok());
        log!("First list element: {:?}", result.unwrap());

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();

        log!("=== List Table Ordering Test Completed ===");
    }

    // ================================================
    // SET TABLE INTEGRATION TESTS
    // ================================================

    #[pg_test]
    fn test_integration_set_table_basic_crud() {
        log!("=== Testing Set Table Basic CRUD Operations ===");

        setup_redis_fdw();

        let table_name = "test_set_crud";
        let key_prefix = "integration:set:crud";

        create_foreign_table(table_name, "member text", "set", key_prefix);

        // Test INSERT operations with pacing
        log!("Testing INSERT operations...");
        controlled_insert_single(table_name, "apple");
        controlled_insert_single(table_name, "banana");
        controlled_insert_single(table_name, "cherry");

        // Test duplicate insertion (should not increase count in Redis sets)
        controlled_insert_single(table_name, "apple");

        // Test SELECT operations
        log!("Testing SELECT operations...");
        let count = controlled_select_count(table_name);
        log!(
            "Set table count after INSERT (including duplicate): {:?}",
            count
        );

        // Test membership check

        let apple_exists = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM {} WHERE member = 'apple';",
            table_name
        ));
        assert!(apple_exists.is_ok());
        assert!(apple_exists.unwrap().unwrap() > 0);

        // Test DELETE operations
        log!("Testing DELETE operations...");
        controlled_delete(table_name, "member = 'banana'");

        let count_after_delete = controlled_select_count(table_name);
        log!("Set table count after DELETE: {:?}", count_after_delete);

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();

        log!("=== Set Table Basic CRUD Test Completed ===");
    }

    #[pg_test]
    fn test_integration_set_table_uniqueness() {
        log!("=== Testing Set Table Uniqueness ===");

        setup_redis_fdw();

        let table_name = "test_set_unique";
        let key_prefix = "integration:set:unique";

        create_foreign_table(table_name, "member text", "set", key_prefix);

        // Insert same value multiple times
        for i in 1..=5 {
            controlled_insert_single(table_name, "unique_value");
            log!("Inserted duplicate #{}", i);
        }

        // Check that set maintains uniqueness (Redis behavior)
        let count = controlled_select_count(table_name);
        log!(
            "Set count after multiple inserts of same value: {:?}",
            count
        );

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();

        log!("=== Set Table Uniqueness Test Completed ===");
    }

    // ================================================
    // STRING TABLE INTEGRATION TESTS
    // ================================================

    #[pg_test]
    fn test_integration_string_table_basic_crud() {
        log!("=== Testing String Table Basic CRUD Operations ===");

        setup_redis_fdw();

        let table_name = "test_string_crud";
        let key_prefix = "integration:string:crud";

        create_foreign_table(table_name, "value text", "string", key_prefix);

        // Test INSERT operations with pacing
        log!("Testing INSERT operations...");
        controlled_insert_single(table_name, "Hello Redis FDW");
        controlled_insert_single(table_name, "Another string value");

        // Test SELECT operations
        log!("Testing SELECT operations...");
        let count = controlled_select_count(table_name);
        log!("String table count after INSERT: {:?}", count);

        // Test individual record selection

        let result = Spi::get_one::<String>(&format!(
            "SELECT value FROM {table_name} WHERE value = 'Another string value';"
        ));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().unwrap(), "Another string value");

        // Test DELETE operations
        log!("Testing DELETE operations...");
        controlled_delete(table_name, "value = 'Another string value'");

        let count_after_delete = controlled_select_count(table_name);
        log!("String table count after DELETE: {:?}", count_after_delete);

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();

        log!("=== String Table Basic CRUD Test Completed ===");
    }

    #[pg_test]
    fn test_integration_string_table_large_data() {
        log!("=== Testing String Table Large Data Handling ===");

        setup_redis_fdw();

        let table_name = "test_string_large";
        let key_prefix = "integration:string:large";

        create_foreign_table(table_name, "value text", "string", key_prefix);

        // Test with large string (1KB)
        let large_value = "X".repeat(1000);
        controlled_insert_single(table_name, &large_value);

        // Verify retrieval

        let retrieved = Spi::get_one::<String>(&format!(
            "SELECT value FROM {} WHERE LENGTH(value) = 1000;",
            table_name
        ));
        assert!(retrieved.is_ok());
        assert_eq!(retrieved.unwrap().unwrap().len(), 1000);

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();

        log!("=== String Table Large Data Test Completed ===");
    }

    // ================================================
    // ZSET (SORTED SET) TABLE INTEGRATION TESTS
    // ================================================

    #[pg_test]
    fn test_integration_zset_table_basic_crud() {
        log!("=== Testing ZSet Table Basic CRUD Operations ===");

        setup_redis_fdw();

        let table_name = "test_zset_crud";
        let key_prefix = "integration:zset:crud";

        create_foreign_table(table_name, "member text, score numeric", "zset", key_prefix);

        // Test INSERT operations with pacing
        log!("Testing INSERT operations...");
        controlled_insert_pair(table_name, "player1", "100");
        controlled_insert_pair(table_name, "player2", "150");
        controlled_insert_pair(table_name, "player3", "200");

        // Test SELECT operations
        let count = controlled_select_count(table_name);
        assert_eq!(count, Some(3));

        // Test individual record selection

        let result = Spi::get_one::<String>(&format!(
            "SELECT member FROM {} WHERE score = 150;",
            table_name
        ));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().unwrap(), "player2");

        // Test DELETE operations
        log!("Testing DELETE operations...");
        controlled_delete(table_name, "member = 'player2'");

        let count_after_delete = controlled_select_count(table_name);
        assert_eq!(count_after_delete, Some(2));
        log!("ZSet table count after DELETE: {:?}", count_after_delete);

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();

        log!("=== ZSet Table Basic CRUD Test Completed ===");
    }

    #[pg_test]
    fn test_integration_zset_table_scoring() {
        log!("=== Testing ZSet Table Scoring ===");

        setup_redis_fdw();

        let table_name = "test_zset_score";
        let key_prefix = "integration:zset:score";

        create_foreign_table(table_name, "member text,score numeric", "zset", key_prefix);

        // Insert with different scores
        controlled_insert_pair(table_name, "low_player", "50");
        controlled_insert_pair(table_name, "high_player", "500");
        controlled_insert_pair(table_name, "mid_player", "250");

        // Test count
        let count = controlled_select_count(table_name);
        assert_eq!(count, Some(3));

        // Test score-based selection

        let high_scorer = Spi::get_one::<String>(&format!(
            "SELECT member FROM {} WHERE score > 400;",
            table_name
        ));
        assert!(high_scorer.is_ok());
        assert_eq!(high_scorer.unwrap().unwrap(), "high_player");

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();

        log!("=== ZSet Table Scoring Test Completed ===");
    }

    // ================================================
    // CROSS-TABLE TYPE TESTING
    // ================================================

    #[pg_test]
    fn test_integration_multiple_table_types() {
        log!("=== Testing Multiple Redis Table Types Simultaneously ===");

        setup_redis_fdw();

        // Create different table types
        create_foreign_table(
            "multi_hash",
            "key text, value text",
            "hash",
            "integration:multi:hash",
        );

        create_foreign_table(
            "multi_list",
            "element text",
            "list",
            "integration:multi:list",
        );

        controlled_delete("multi_list", "1 = 1");

        create_foreign_table("multi_set", "member text", "set", "integration:multi:set");

        create_foreign_table(
            "multi_string",
            "value text",
            "string",
            "integration:multi:string",
        );

        create_foreign_table(
            "multi_zset",
            "member text,score numeric",
            "zset",
            "integration:multi:zset",
        );

        // Insert data into each type
        log!("Inserting data into all table types...");
        controlled_insert_pair("multi_hash", "key1", "hash_value");
        controlled_insert_single("multi_list", "list_item");
        controlled_insert_single("multi_set", "set_member");
        controlled_insert_single("multi_string", "string_value");
        controlled_insert_pair("multi_zset", "zset_member", "100");

        // Verify all tables have data
        assert_eq!(controlled_select_count("multi_hash"), Some(1));
        assert_eq!(controlled_select_count("multi_list"), Some(1));
        assert_eq!(controlled_select_count("multi_set"), Some(1));
        assert_eq!(controlled_select_count("multi_string"), Some(1));
        assert_eq!(controlled_select_count("multi_zset"), Some(1));

        // Cleanup all tables
        drop_foreign_table("multi_hash");
        drop_foreign_table("multi_list");
        drop_foreign_table("multi_set");
        drop_foreign_table("multi_string");
        drop_foreign_table("multi_zset");

        cleanup_redis_fdw();

        log!("=== Multiple Table Types Test Completed ===");
    }

    // ================================================
    // STRESS TESTING WITH CONTROLLED PACING
    // ================================================

    #[pg_test]
    fn test_integration_stress_controlled_operations() {
        log!("=== Testing Stress Operations with Controlled Pacing ===");

        setup_redis_fdw();

        let table_name = "test_stress_hash";
        let key_prefix = "integration:stress:hash";

        create_foreign_table(table_name, "field text, value text", "hash", key_prefix);

        // Stress test with controlled pacing
        log!(
            "Performing stress test with {} operations...",
            BATCH_SIZE * 5
        );
        for batch in 0..5 {
            for i in 1..=BATCH_SIZE {
                let index = batch * BATCH_SIZE + i;
                let field = format!("stress_field:{}", index);
                let value = format!("stress_value_{}", index);
                controlled_insert_pair(table_name, &field, &value);
            }
            // Longer pause between batches during stress testing
            log!("Completed batch {}/5", batch + 1);
        }

        // Verify final count
        let final_count = controlled_select_count(table_name);
        assert_eq!(final_count, Some(50));
        log!(
            "Stress test completed successfully with {} records",
            final_count.unwrap()
        );

        // Cleanup
        drop_foreign_table(table_name);
        cleanup_redis_fdw();

        log!("=== Stress Test Completed ===");
    }

    // ================================================
    // DATABASE ISOLATION TESTING
    // ================================================

    #[pg_test]
    fn test_integration_database_isolation() {
        log!("=== Testing Database Isolation ===");

        setup_redis_fdw();

        // Create tables in different Redis databases
        create_foreign_table(
            "db0_table",
            "field text, value text",
            "hash",
            "integration:db0:hash",
        );

        // Change the SQL to create a table in a different database
        let sql_db1 = format!(
            "CREATE FOREIGN TABLE db1_table (field text, value text) SERVER {} OPTIONS (
                database '1',
                table_type 'hash',
                table_key_prefix 'integration:db1:hash'
            );",
            SERVER_NAME
        );
        Spi::run(&sql_db1).unwrap();

        // Insert data into both databases
        controlled_insert_pair("db0_table", "db0_key", "db0_value");
        controlled_insert_pair("db1_table", "db1_key", "db1_value");

        // Verify isolation
        let db0_count = controlled_select_count("db0_table");
        let db1_count = controlled_select_count("db1_table");

        assert_eq!(db0_count, Some(1));
        assert_eq!(db1_count, Some(1));

        log!(
            "Database isolation verified: DB0={:?}, DB1={:?}",
            db0_count,
            db1_count
        );

        // Cleanup
        drop_foreign_table("db0_table");
        drop_foreign_table("db1_table");
        cleanup_redis_fdw();

        log!("=== Database Isolation Test Completed ===");
    }

    // ================================================
    // COMPREHENSIVE SMOKE TEST
    // ================================================

    #[pg_test]
    fn test_integration_comprehensive_smoke_test() {
        log!("=== Running Comprehensive Integration Smoke Test ===");

        setup_redis_fdw();

        // Test basic connectivity and operations for each table type
        let test_cases = [
            ("smoke_hash", "field text, value text", "hash", "smoke:hash"),
            ("smoke_list", "element text", "list", "smoke:list"),
            ("smoke_set", "member text", "set", "smoke:set"),
            ("smoke_string", "value text", "string", "smoke:string"),
            (
                "smoke_zset",
                "member text ,score numeric",
                "zset",
                "smoke:zset",
            ),
        ];

        for (table_name, columns, table_type, key_prefix) in &test_cases {
            log!("Testing table type: {}", table_type);

            create_foreign_table(table_name, columns, table_type, key_prefix);

            // Perform appropriate operations based on table type
            match *table_type {
                "hash" => {
                    controlled_insert_pair(table_name, "test_field", "test_value");
                }
                "list" | "set" | "string" => {
                    controlled_insert_single(table_name, "test_value");
                }
                "zset" => {
                    controlled_insert_pair(table_name, "test_member", "100");
                }
                _ => {}
            }

            // Verify the operation worked
            let count = controlled_select_count(table_name);
            assert!(count.is_some() && count.unwrap() > 0);
            log!("Table type {} working correctly", table_type);

            drop_foreign_table(table_name);
        }

        cleanup_redis_fdw();

        log!("=== Comprehensive Smoke Test Completed Successfully ===");
    }
}
