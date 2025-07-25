/// Redis Cluster Integration Tests
///
/// This module contains comprehensive integration tests for Redis cluster functionality.
/// These tests verify that the Redis FDW works correctly with Redis clusters,
/// testing all table types, operations, and edge cases.
///
/// Prerequisites:
/// - Redis cluster running via Docker Compose (use scripts/cluster_test.sh start)
/// - Environment variable REDIS_CLUSTER_NODES set to cluster endpoints
///
/// To run cluster tests:
/// ```bash
/// ./scripts/cluster_test.sh start
/// ./scripts/cluster_test.sh test
/// ```

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;
    use std::env;
    use std::thread;
    use std::time::Duration;

    /// Test configuration constants
    const DEFAULT_CLUSTER_NODES: &str = "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006";
    const TEST_DATABASE: &str = "0";
    const FDW_NAME: &str = "redis_cluster_test_wrapper";
    const SERVER_NAME: &str = "redis_cluster_test_server";

    /// Connection management constants
    const OPERATION_DELAY_MS: u64 = 50; // 50ms between operations
    const BATCH_SIZE: usize = 5; // Process operations in smaller batches for cluster
    const BATCH_DELAY_MS: u64 = 100; // 100ms between batches

    /// Get cluster nodes configuration
    fn get_cluster_nodes() -> String {
        env::var("REDIS_CLUSTER_NODES")
            .unwrap_or_else(|_| DEFAULT_CLUSTER_NODES.to_string())
    }

    /// Setup helper to create FDW and server for cluster with error handling
    fn setup_redis_cluster_fdw() {
        log!("Setting up Redis cluster FDW connection...");

        let cluster_nodes = get_cluster_nodes();

        // Clean up any existing FDW/server first
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            FDW_NAME
        ));

        // Small delay to ensure cleanup completes
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        // Create FDW
        Spi::run(&format!(
            "CREATE FOREIGN DATA WRAPPER {} HANDLER redis_fdw_handler;",
            FDW_NAME
        ))
        .unwrap();

        // Create server with cluster nodes
        Spi::run(&format!(
            "CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (host_port '{}');",
            SERVER_NAME, FDW_NAME, cluster_nodes
        ))
        .unwrap();

        log!("Redis cluster FDW setup completed successfully with nodes: {}", cluster_nodes);
    }

    /// Cleanup helper to remove FDW and server
    fn cleanup_redis_cluster_fdw() {
        log!("Cleaning up Redis cluster FDW connection...");
        let _ = Spi::run(&format!("DROP SERVER IF EXISTS {} CASCADE;", SERVER_NAME));
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            FDW_NAME
        ));

        // Small delay to ensure cleanup completes
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        log!("Redis cluster FDW cleanup completed");
    }

    /// Helper to create a foreign table with specified options for cluster
    fn create_cluster_foreign_table(table_name: &str, columns: &str, table_type: &str, key_prefix: &str) {
        let sql = format!(
            "CREATE FOREIGN TABLE {table_name} ({columns}) SERVER {SERVER_NAME} OPTIONS (
                database '{TEST_DATABASE}',
                table_type '{table_type}',
                table_key_prefix '{key_prefix}'
            );"
        );
        Spi::run(&sql).unwrap();
        log!("Created cluster foreign table: {table_name} of type: {table_type}");

        // Small delay after table creation
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
    }

    /// Helper to drop a foreign table
    fn drop_cluster_foreign_table(table_name: &str) {
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {table_name};"));
        log!("Dropped cluster foreign table: {table_name}");

        // Small delay after table drop
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
    }

    /// Test cluster connectivity and basic operations
    #[pg_test]
    fn test_cluster_connectivity() {
        setup_redis_cluster_fdw();

        // Test with a simple string table
        create_cluster_foreign_table(
            "cluster_connectivity_test",
            "value TEXT",
            "string",
            "conn_test:test1"
        );

        // Test basic operations
        Spi::run("INSERT INTO cluster_connectivity_test (value) VALUES ('cluster_value1');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        let result = Spi::get_one::<String>("SELECT value FROM cluster_connectivity_test;")
            .expect("Failed to retrieve value from cluster");

        assert_eq!(result, Some("cluster_value1".to_string()));

        drop_cluster_foreign_table("cluster_connectivity_test");
        cleanup_redis_cluster_fdw();
    }

    /// Test string table operations across cluster
    #[pg_test]
    fn test_cluster_string_table() {
        setup_redis_cluster_fdw();

        // Create multiple string tables to test different keys
        create_cluster_foreign_table(
            "cluster_string_test1",
            "value TEXT",
            "string",
            "str_cluster:key1"
        );
        
        create_cluster_foreign_table(
            "cluster_string_test2",
            "value TEXT",
            "string",
            "str_cluster:key2"
        );
        
        create_cluster_foreign_table(
            "cluster_string_test3",
            "value TEXT",
            "string",
            "str_cluster:key3"
        );

        // Insert values into different string tables (different Redis keys)
        Spi::run("INSERT INTO cluster_string_test1 (value) VALUES ('value1');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        
        Spi::run("INSERT INTO cluster_string_test2 (value) VALUES ('value2');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        
        Spi::run("INSERT INTO cluster_string_test3 (value) VALUES ('value3');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        // Verify all values can be retrieved
        let result1 = Spi::get_one::<String>("SELECT value FROM cluster_string_test1;")
            .expect("Failed to retrieve value from cluster");
        assert_eq!(result1, Some("value1".to_string()));
        
        let result2 = Spi::get_one::<String>("SELECT value FROM cluster_string_test2;")
            .expect("Failed to retrieve value from cluster");
        assert_eq!(result2, Some("value2".to_string()));
        
        let result3 = Spi::get_one::<String>("SELECT value FROM cluster_string_test3;")
            .expect("Failed to retrieve value from cluster");
        assert_eq!(result3, Some("value3".to_string()));

        drop_cluster_foreign_table("cluster_string_test1");
        drop_cluster_foreign_table("cluster_string_test2");
        drop_cluster_foreign_table("cluster_string_test3");
        cleanup_redis_cluster_fdw();
    }

    /// Test hash table operations across cluster
    #[pg_test]
    fn test_cluster_hash_table() {
        setup_redis_cluster_fdw();

        // Create hash tables for different users
        create_cluster_foreign_table(
            "cluster_hash_user1",
            "field TEXT, value TEXT",
            "hash",
            "hash_cluster:user:1"
        );
        
        create_cluster_foreign_table(
            "cluster_hash_user2",
            "field TEXT, value TEXT",
            "hash",
            "hash_cluster:user:2"
        );
        
        create_cluster_foreign_table(
            "cluster_hash_user3",
            "field TEXT, value TEXT",
            "hash",
            "hash_cluster:user:3"
        );

        // Insert hash data for user:1
        Spi::run("INSERT INTO cluster_hash_user1 (field, value) VALUES ('name', 'Alice');").unwrap();
        Spi::run("INSERT INTO cluster_hash_user1 (field, value) VALUES ('age', '30');").unwrap();
        
        // Insert hash data for user:2
        Spi::run("INSERT INTO cluster_hash_user2 (field, value) VALUES ('name', 'Bob');").unwrap();
        Spi::run("INSERT INTO cluster_hash_user2 (field, value) VALUES ('age', '25');").unwrap();
        
        // Insert hash data for user:3
        Spi::run("INSERT INTO cluster_hash_user3 (field, value) VALUES ('name', 'Charlie');").unwrap();
        Spi::run("INSERT INTO cluster_hash_user3 (field, value) VALUES ('age', '35');").unwrap();

        // Verify hash values
        let alice_name = Spi::get_one::<String>(
            "SELECT value FROM cluster_hash_user1 WHERE field = 'name';"
        ).expect("Failed to retrieve hash value");
        assert_eq!(alice_name, Some("Alice".to_string()));

        let bob_age = Spi::get_one::<String>(
            "SELECT value FROM cluster_hash_user2 WHERE field = 'age';"
        ).expect("Failed to retrieve hash value");
        assert_eq!(bob_age, Some("25".to_string()));

        // Test retrieving all fields for a user
        let user1_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_hash_user1;"
        ).expect("Failed to count hash fields");
        assert_eq!(user1_count, Some(2));

        drop_cluster_foreign_table("cluster_hash_user1");
        drop_cluster_foreign_table("cluster_hash_user2");
        drop_cluster_foreign_table("cluster_hash_user3");
        cleanup_redis_cluster_fdw();
    }

    /// Test list table operations across cluster
    #[pg_test]
    fn test_cluster_list_table() {
        setup_redis_cluster_fdw();

        // Create list tables for different task queues
        create_cluster_foreign_table(
            "cluster_list_urgent",
            "element TEXT",
            "list",
            "list_cluster:tasks:urgent"
        );
        
        create_cluster_foreign_table(
            "cluster_list_normal",
            "element TEXT",
            "list",
            "list_cluster:tasks:normal"
        );
        
        create_cluster_foreign_table(
            "cluster_list_low",
            "element TEXT",
            "list",
            "list_cluster:tasks:low"
        );

        // Insert list data
        Spi::run("INSERT INTO cluster_list_urgent (element) VALUES ('task1');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        
        Spi::run("INSERT INTO cluster_list_urgent (element) VALUES ('task2');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        
        Spi::run("INSERT INTO cluster_list_normal (element) VALUES ('task3');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        
        Spi::run("INSERT INTO cluster_list_normal (element) VALUES ('task4');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        
        Spi::run("INSERT INTO cluster_list_low (element) VALUES ('task5');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        // Verify list contents
        let urgent_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_list_urgent;"
        ).expect("Failed to count list items");
        assert_eq!(urgent_count, Some(2));

        let normal_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_list_normal;"
        ).expect("Failed to count list items");
        assert_eq!(normal_count, Some(2));
        
        let low_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_list_low;"
        ).expect("Failed to count list items");
        assert_eq!(low_count, Some(1));

        drop_cluster_foreign_table("cluster_list_urgent");
        drop_cluster_foreign_table("cluster_list_normal");
        drop_cluster_foreign_table("cluster_list_low");
        cleanup_redis_cluster_fdw();
    }

    /// Test set table operations across cluster
    #[pg_test]
    fn test_cluster_set_table() {
        setup_redis_cluster_fdw();

        // Create set tables for different tag categories
        create_cluster_foreign_table(
            "cluster_set_frontend",
            "member TEXT",
            "set",
            "set_cluster:tags:frontend"
        );
        
        create_cluster_foreign_table(
            "cluster_set_backend",
            "member TEXT",
            "set",
            "set_cluster:tags:backend"
        );

        // Insert set data
        Spi::run("INSERT INTO cluster_set_frontend (member) VALUES ('javascript');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        
        Spi::run("INSERT INTO cluster_set_frontend (member) VALUES ('react');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        
        Spi::run("INSERT INTO cluster_set_frontend (member) VALUES ('css');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        
        Spi::run("INSERT INTO cluster_set_backend (member) VALUES ('rust');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        
        Spi::run("INSERT INTO cluster_set_backend (member) VALUES ('postgresql');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        
        Spi::run("INSERT INTO cluster_set_backend (member) VALUES ('redis');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        // Verify set contents
        let frontend_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_set_frontend;"
        ).expect("Failed to count set members");
        assert_eq!(frontend_count, Some(3));

        let backend_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_set_backend;"
        ).expect("Failed to count set members");
        assert_eq!(backend_count, Some(3));

        // Test duplicate insertion (should not increase count)
        Spi::run("INSERT INTO cluster_set_frontend (member) VALUES ('javascript');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        let frontend_count_after = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_set_frontend;"
        ).expect("Failed to count set members");
        assert_eq!(frontend_count_after, Some(3)); // Should still be 3

        drop_cluster_foreign_table("cluster_set_frontend");
        drop_cluster_foreign_table("cluster_set_backend");
        cleanup_redis_cluster_fdw();
    }

    /// Test sorted set table operations across cluster
    #[pg_test]
    fn test_cluster_zset_table() {
        setup_redis_cluster_fdw();

        // Create sorted set tables for different game leaderboards
        create_cluster_foreign_table(
            "cluster_zset_game1",
            "member TEXT, score FLOAT8",
            "zset",
            "zset_cluster:leaderboard:game1"
        );
        
        create_cluster_foreign_table(
            "cluster_zset_game2",
            "member TEXT, score FLOAT8",
            "zset",
            "zset_cluster:leaderboard:game2"
        );

        // Insert sorted set data
        Spi::run("INSERT INTO cluster_zset_game1 (member, score) VALUES ('player1', 1000.0);").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        
        Spi::run("INSERT INTO cluster_zset_game1 (member, score) VALUES ('player2', 950.0);").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        
        Spi::run("INSERT INTO cluster_zset_game1 (member, score) VALUES ('player3', 1100.0);").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        
        Spi::run("INSERT INTO cluster_zset_game2 (member, score) VALUES ('player4', 800.0);").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        
        Spi::run("INSERT INTO cluster_zset_game2 (member, score) VALUES ('player5', 1200.0);").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        // Verify sorted set contents
        let game1_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_zset_game1;"
        ).expect("Failed to count zset members");
        assert_eq!(game1_count, Some(3));

        let game2_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_zset_game2;"
        ).expect("Failed to count zset members");
        assert_eq!(game2_count, Some(2));

        // Test that scores are stored correctly
        let player3_score = Spi::get_one::<f64>(
            "SELECT score FROM cluster_zset_game1 WHERE member = 'player3';"
        ).expect("Failed to retrieve score");
        assert_eq!(player3_score, Some(1100.0));

        drop_cluster_foreign_table("cluster_zset_game1");
        drop_cluster_foreign_table("cluster_zset_game2");
        cleanup_redis_cluster_fdw();
    }

    /// Test mixed operations across multiple table types in cluster
    #[pg_test]
    fn test_cluster_mixed_operations() {
        setup_redis_cluster_fdw();

        // Create multiple table types with correct schemas
        create_cluster_foreign_table(
            "cluster_mixed_string",
            "value TEXT",
            "string",
            "mixed_str:config:app"
        );

        create_cluster_foreign_table(
            "cluster_mixed_hash",
            "field TEXT, value TEXT",
            "hash",
            "mixed_hash:user:settings"
        );

        create_cluster_foreign_table(
            "cluster_mixed_set",
            "member TEXT",
            "set",
            "mixed_set:permissions:admin"
        );

        // Insert data into all table types
        Spi::run("INSERT INTO cluster_mixed_string (value) VALUES ('production');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        Spi::run("INSERT INTO cluster_mixed_hash (field, value) VALUES ('theme', 'dark');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        Spi::run("INSERT INTO cluster_mixed_set (member) VALUES ('read');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        Spi::run("INSERT INTO cluster_mixed_set (member) VALUES ('write');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        // Verify all data is accessible
        let config_value = Spi::get_one::<String>(
            "SELECT value FROM cluster_mixed_string;"
        ).expect("Failed to retrieve string value");
        assert_eq!(config_value, Some("production".to_string()));

        let theme_value = Spi::get_one::<String>(
            "SELECT value FROM cluster_mixed_hash WHERE field = 'theme';"
        ).expect("Failed to retrieve hash value");
        assert_eq!(theme_value, Some("dark".to_string()));

        let permission_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_mixed_set;"
        ).expect("Failed to count set members");
        assert_eq!(permission_count, Some(2));

        // Cleanup
        drop_cluster_foreign_table("cluster_mixed_string");
        drop_cluster_foreign_table("cluster_mixed_hash");
        drop_cluster_foreign_table("cluster_mixed_set");
        cleanup_redis_cluster_fdw();
    }

    /// Test cluster behavior with key distribution
    #[pg_test]
    fn test_cluster_key_distribution() {
        setup_redis_cluster_fdw();

        // Create multiple string tables to test distribution across cluster nodes
        // Each table represents a different Redis key that will be distributed across nodes
        let table_count = 10;
        let mut table_names = Vec::new();
        
        for i in 1..=table_count {
            let table_name = format!("cluster_dist_test_{}", i);
            create_cluster_foreign_table(
                &table_name,
                "value TEXT",
                "string",
                &format!("dist:key{}", i)
            );
            table_names.push(table_name);
            
            // Insert value into each table
            Spi::run(&format!(
                "INSERT INTO cluster_dist_test_{} (value) VALUES ('value{}');",
                i, i
            )).unwrap();
            
            // Small delay between insertions to allow cluster distribution
            if i % BATCH_SIZE == 0 {
                thread::sleep(Duration::from_millis(BATCH_DELAY_MS));
            } else {
                thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
            }
        }

        // Verify all keys are accessible
        for i in 1..=table_count {
            let result = Spi::get_one::<String>(&format!(
                "SELECT value FROM cluster_dist_test_{};", i
            )).expect("Failed to retrieve distributed key");
            assert_eq!(result, Some(format!("value{}", i)));
        }

        // Cleanup all tables
        for table_name in table_names {
            drop_cluster_foreign_table(&table_name);
        }
        
        cleanup_redis_cluster_fdw();
    }

    /// Test cluster error handling and resilience
    #[pg_test]
    fn test_cluster_error_handling() {
        setup_redis_cluster_fdw();

        create_cluster_foreign_table(
            "cluster_error_test",
            "value TEXT",
            "string",
            "error_test:valid_key"
        );

        // Test successful operation first
        Spi::run("INSERT INTO cluster_error_test (value) VALUES ('valid_value');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        let result = Spi::get_one::<String>("SELECT value FROM cluster_error_test;")
            .expect("Failed to retrieve value");
        assert_eq!(result, Some("valid_value".to_string()));

        // Test with an empty table (different key prefix)
        create_cluster_foreign_table(
            "cluster_error_empty",
            "value TEXT",
            "string",
            "error_test:non_existent_key"
        );

        // Query empty table should return None, not error
        let non_existent = Spi::get_one::<String>("SELECT value FROM cluster_error_empty;");
        assert_eq!(non_existent, Err(pgrx::spi::SpiError::InvalidPosition));

        drop_cluster_foreign_table("cluster_error_test");
        drop_cluster_foreign_table("cluster_error_empty");
        cleanup_redis_cluster_fdw();
    }
}
