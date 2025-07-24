/// Redis Cluster Integration Tests
///
/// This module contains comprehensive integration tests for Redis cluster functionality.
/// These tests verify that the Redis FDW works correctly with Redis clusters,
/// testing all table types, operations, and edge cases.
///
/// Prerequisites:
/// - Redis cluster running via Docker Compose (use scripts/cluster_test.sh start)
/// - Environment variable REDIS_CLUSTER_NODES set to cluster endpoints
/// - Environment variable REDIS_CLUSTER_TEST_ENABLED=true to enable cluster tests
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

    /// Check if cluster tests are enabled and cluster is available
    fn is_cluster_test_enabled() -> bool {
        // Check if cluster tests are enabled
        if env::var("REDIS_CLUSTER_TEST_ENABLED").unwrap_or_default() != "true" {
            return false;
        }

        // Check if cluster nodes are configured
        let _cluster_nodes = env::var("REDIS_CLUSTER_NODES")
            .unwrap_or_else(|_| DEFAULT_CLUSTER_NODES.to_string());

        // TODO: Could add actual connectivity check here
        true
    }

    /// Get cluster nodes configuration
    fn get_cluster_nodes() -> String {
        env::var("REDIS_CLUSTER_NODES")
            .unwrap_or_else(|_| DEFAULT_CLUSTER_NODES.to_string())
    }

    /// Setup helper to create FDW and server for cluster with error handling
    fn setup_redis_cluster_fdw() {
        if !is_cluster_test_enabled() {
            return;
        }

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
        if !is_cluster_test_enabled() {
            return;
        }

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
        if !is_cluster_test_enabled() {
            log!("Cluster tests disabled, skipping test_cluster_connectivity");
            return;
        }

        setup_redis_cluster_fdw();

        // Test with a simple string table
        create_cluster_foreign_table(
            "cluster_connectivity_test",
            "key TEXT, value TEXT",
            "string",
            "conn_test:"
        );

        // Test basic operations
        Spi::run("INSERT INTO cluster_connectivity_test (key, value) VALUES ('test1', 'cluster_value1');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        let result = Spi::get_one::<String>("SELECT value FROM cluster_connectivity_test WHERE key = 'test1';")
            .expect("Failed to retrieve value from cluster");

        assert_eq!(result, Some("cluster_value1".to_string()));

        drop_cluster_foreign_table("cluster_connectivity_test");
        cleanup_redis_cluster_fdw();
    }

    /// Test string table operations across cluster
    #[pg_test]
    fn test_cluster_string_table() {
        if !is_cluster_test_enabled() {
            log!("Cluster tests disabled, skipping test_cluster_string_table");
            return;
        }

        setup_redis_cluster_fdw();

        create_cluster_foreign_table(
            "cluster_string_test",
            "key TEXT, value TEXT",
            "string",
            "str_cluster:"
        );

        // Insert multiple values that will be distributed across cluster nodes
        let test_data = vec![
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
            ("key4", "value4"),
            ("key5", "value5"),
        ];

        for (key, value) in &test_data {
            Spi::run(&format!(
                "INSERT INTO cluster_string_test (key, value) VALUES ('{}', '{}');",
                key, value
            )).unwrap();
            thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        }

        // Verify all values can be retrieved
        for (key, expected_value) in &test_data {
            let result = Spi::get_one::<String>(&format!(
                "SELECT value FROM cluster_string_test WHERE key = '{}';",
                key
            )).expect("Failed to retrieve value from cluster");

            assert_eq!(result, Some(expected_value.to_string()));
        }

        // Test batch retrieval
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM cluster_string_test;")
            .expect("Failed to count rows");
        assert_eq!(count, Some(test_data.len() as i64));

        drop_cluster_foreign_table("cluster_string_test");
        cleanup_redis_cluster_fdw();
    }

    /// Test hash table operations across cluster
    #[pg_test]
    fn test_cluster_hash_table() {
        if !is_cluster_test_enabled() {
            log!("Cluster tests disabled, skipping test_cluster_hash_table");
            return;
        }

        setup_redis_cluster_fdw();

        create_cluster_foreign_table(
            "cluster_hash_test",
            "key TEXT, field TEXT, value TEXT",
            "hash",
            "hash_cluster:"
        );

        // Insert hash data across cluster
        let hash_data = vec![
            ("user:1", "name", "Alice"),
            ("user:1", "age", "30"),
            ("user:2", "name", "Bob"),
            ("user:2", "age", "25"),
            ("user:3", "name", "Charlie"),
            ("user:3", "age", "35"),
        ];

        for (key, field, value) in &hash_data {
            Spi::run(&format!(
                "INSERT INTO cluster_hash_test (key, field, value) VALUES ('{}', '{}', '{}');",
                key, field, value
            )).unwrap();
            thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        }

        // Verify hash values
        let alice_name = Spi::get_one::<String>(
            "SELECT value FROM cluster_hash_test WHERE key = 'user:1' AND field = 'name';"
        ).expect("Failed to retrieve hash value");
        assert_eq!(alice_name, Some("Alice".to_string()));

        let bob_age = Spi::get_one::<String>(
            "SELECT value FROM cluster_hash_test WHERE key = 'user:2' AND field = 'age';"
        ).expect("Failed to retrieve hash value");
        assert_eq!(bob_age, Some("25".to_string()));

        // Test retrieving all fields for a key
        let user1_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_hash_test WHERE key = 'user:1';"
        ).expect("Failed to count hash fields");
        assert_eq!(user1_count, Some(2));

        drop_cluster_foreign_table("cluster_hash_test");
        cleanup_redis_cluster_fdw();
    }

    /// Test list table operations across cluster
    #[pg_test]
    fn test_cluster_list_table() {
        if !is_cluster_test_enabled() {
            log!("Cluster tests disabled, skipping test_cluster_list_table");
            return;
        }

        setup_redis_cluster_fdw();

        create_cluster_foreign_table(
            "cluster_list_test",
            "key TEXT, value TEXT",
            "list",
            "list_cluster:"
        );

        // Insert list data
        let list_data = vec![
            ("tasks:urgent", "task1"),
            ("tasks:urgent", "task2"),
            ("tasks:normal", "task3"),
            ("tasks:normal", "task4"),
            ("tasks:low", "task5"),
        ];

        for (key, value) in &list_data {
            Spi::run(&format!(
                "INSERT INTO cluster_list_test (key, value) VALUES ('{}', '{}');",
                key, value
            )).unwrap();
            thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        }

        // Verify list contents
        let urgent_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_list_test WHERE key = 'tasks:urgent';"
        ).expect("Failed to count list items");
        assert_eq!(urgent_count, Some(2));

        let normal_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_list_test WHERE key = 'tasks:normal';"
        ).expect("Failed to count list items");
        assert_eq!(normal_count, Some(2));

        drop_cluster_foreign_table("cluster_list_test");
        cleanup_redis_cluster_fdw();
    }

    /// Test set table operations across cluster
    #[pg_test]
    fn test_cluster_set_table() {
        if !is_cluster_test_enabled() {
            log!("Cluster tests disabled, skipping test_cluster_set_table");
            return;
        }

        setup_redis_cluster_fdw();

        create_cluster_foreign_table(
            "cluster_set_test",
            "key TEXT, member TEXT",
            "set",
            "set_cluster:"
        );

        // Insert set data
        let set_data = vec![
            ("tags:frontend", "javascript"),
            ("tags:frontend", "react"),
            ("tags:frontend", "css"),
            ("tags:backend", "rust"),
            ("tags:backend", "postgresql"),
            ("tags:backend", "redis"),
        ];

        for (key, member) in &set_data {
            Spi::run(&format!(
                "INSERT INTO cluster_set_test (key, member) VALUES ('{}', '{}');",
                key, member
            )).unwrap();
            thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        }

        // Verify set contents
        let frontend_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_set_test WHERE key = 'tags:frontend';"
        ).expect("Failed to count set members");
        assert_eq!(frontend_count, Some(3));

        let backend_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_set_test WHERE key = 'tags:backend';"
        ).expect("Failed to count set members");
        assert_eq!(backend_count, Some(3));

        // Test duplicate insertion (should not increase count)
        Spi::run("INSERT INTO cluster_set_test (key, member) VALUES ('tags:frontend', 'javascript');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        let frontend_count_after = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_set_test WHERE key = 'tags:frontend';"
        ).expect("Failed to count set members");
        assert_eq!(frontend_count_after, Some(3)); // Should still be 3

        drop_cluster_foreign_table("cluster_set_test");
        cleanup_redis_cluster_fdw();
    }

    /// Test sorted set table operations across cluster
    #[pg_test]
    fn test_cluster_zset_table() {
        if !is_cluster_test_enabled() {
            log!("Cluster tests disabled, skipping test_cluster_zset_table");
            return;
        }

        setup_redis_cluster_fdw();

        create_cluster_foreign_table(
            "cluster_zset_test",
            "key TEXT, score FLOAT, member TEXT",
            "zset",
            "zset_cluster:"
        );

        // Insert sorted set data
        let zset_data = vec![
            ("leaderboard:game1", 1000.0, "player1"),
            ("leaderboard:game1", 950.0, "player2"),
            ("leaderboard:game1", 1100.0, "player3"),
            ("leaderboard:game2", 800.0, "player4"),
            ("leaderboard:game2", 1200.0, "player5"),
        ];

        for (key, score, member) in &zset_data {
            Spi::run(&format!(
                "INSERT INTO cluster_zset_test (key, score, member) VALUES ('{}', {}, '{}');",
                key, score, member
            )).unwrap();
            thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
        }

        // Verify sorted set contents
        let game1_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_zset_test WHERE key = 'leaderboard:game1';"
        ).expect("Failed to count zset members");
        assert_eq!(game1_count, Some(3));

        // Test that scores are stored correctly
        let player3_score = Spi::get_one::<f64>(
            "SELECT score FROM cluster_zset_test WHERE key = 'leaderboard:game1' AND member = 'player3';"
        ).expect("Failed to retrieve score");
        assert_eq!(player3_score, Some(1100.0));

        drop_cluster_foreign_table("cluster_zset_test");
        cleanup_redis_cluster_fdw();
    }

    /// Test mixed operations across multiple table types in cluster
    #[pg_test]
    fn test_cluster_mixed_operations() {
        if !is_cluster_test_enabled() {
            log!("Cluster tests disabled, skipping test_cluster_mixed_operations");
            return;
        }

        setup_redis_cluster_fdw();

        // Create multiple table types
        create_cluster_foreign_table(
            "cluster_mixed_string",
            "key TEXT, value TEXT",
            "string",
            "mixed_str:"
        );

        create_cluster_foreign_table(
            "cluster_mixed_hash",
            "key TEXT, field TEXT, value TEXT",
            "hash",
            "mixed_hash:"
        );

        create_cluster_foreign_table(
            "cluster_mixed_set",
            "key TEXT, member TEXT",
            "set",
            "mixed_set:"
        );

        // Insert data into all table types
        Spi::run("INSERT INTO cluster_mixed_string (key, value) VALUES ('config:app', 'production');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        Spi::run("INSERT INTO cluster_mixed_hash (key, field, value) VALUES ('user:settings', 'theme', 'dark');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        Spi::run("INSERT INTO cluster_mixed_set (key, member) VALUES ('permissions:admin', 'read');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        Spi::run("INSERT INTO cluster_mixed_set (key, member) VALUES ('permissions:admin', 'write');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        // Verify all data is accessible
        let config_value = Spi::get_one::<String>(
            "SELECT value FROM cluster_mixed_string WHERE key = 'config:app';"
        ).expect("Failed to retrieve string value");
        assert_eq!(config_value, Some("production".to_string()));

        let theme_value = Spi::get_one::<String>(
            "SELECT value FROM cluster_mixed_hash WHERE key = 'user:settings' AND field = 'theme';"
        ).expect("Failed to retrieve hash value");
        assert_eq!(theme_value, Some("dark".to_string()));

        let permission_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM cluster_mixed_set WHERE key = 'permissions:admin';"
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
        if !is_cluster_test_enabled() {
            log!("Cluster tests disabled, skipping test_cluster_key_distribution");
            return;
        }

        setup_redis_cluster_fdw();

        create_cluster_foreign_table(
            "cluster_distribution_test",
            "key TEXT, value TEXT",
            "string",
            "dist:"
        );

        // Insert many keys to ensure distribution across cluster nodes
        let key_count = 20;
        for i in 1..=key_count {
            Spi::run(&format!(
                "INSERT INTO cluster_distribution_test (key, value) VALUES ('key{}', 'value{}');",
                i, i
            )).unwrap();
            
            // Small delay between insertions
            if i % BATCH_SIZE == 0 {
                thread::sleep(Duration::from_millis(BATCH_DELAY_MS));
            } else {
                thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));
            }
        }

        // Verify all keys are accessible
        let total_count = Spi::get_one::<i64>("SELECT COUNT(*) FROM cluster_distribution_test;")
            .expect("Failed to count distributed keys");
        assert_eq!(total_count, Some(key_count as i64));

        // Test retrieving specific keys
        let key10_value = Spi::get_one::<String>(
            "SELECT value FROM cluster_distribution_test WHERE key = 'key10';"
        ).expect("Failed to retrieve specific key");
        assert_eq!(key10_value, Some("value10".to_string()));

        drop_cluster_foreign_table("cluster_distribution_test");
        cleanup_redis_cluster_fdw();
    }

    /// Test cluster error handling and resilience
    #[pg_test]
    fn test_cluster_error_handling() {
        if !is_cluster_test_enabled() {
            log!("Cluster tests disabled, skipping test_cluster_error_handling");
            return;
        }

        setup_redis_cluster_fdw();

        create_cluster_foreign_table(
            "cluster_error_test",
            "key TEXT, value TEXT",
            "string",
            "error_test:"
        );

        // Test successful operation first
        Spi::run("INSERT INTO cluster_error_test (key, value) VALUES ('valid_key', 'valid_value');").unwrap();
        thread::sleep(Duration::from_millis(OPERATION_DELAY_MS));

        let result = Spi::get_one::<String>("SELECT value FROM cluster_error_test WHERE key = 'valid_key';")
            .expect("Failed to retrieve value");
        assert_eq!(result, Some("valid_value".to_string()));

        // Test querying non-existent key (should return None, not error)
        let non_existent = Spi::get_one::<String>("SELECT value FROM cluster_error_test WHERE key = 'non_existent_key';");
        assert_eq!(non_existent, Ok(None));

        drop_cluster_foreign_table("cluster_error_test");
        cleanup_redis_cluster_fdw();
    }
}
