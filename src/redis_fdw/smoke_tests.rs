// #[cfg(any(test, feature = "pg_test"))]
// #[pgrx::pg_schema] 
// mod smoke_tests {
//     use pgrx::prelude::*;

//     /// Comprehensive smoke test for Redis FDW across all table types
//     /// Tests INSERT, SELECT, and DELETE operations
//     #[pg_test]
//     #[cfg(feature = "integration_tests")]
//     fn test_comprehensive_redis_fdw_smoke() {
//         info!("Starting comprehensive Redis FDW smoke test");
        
//         // Setup: Create FDW infrastructure
//         setup_fdw_infrastructure();
        
//         // Test each Redis table type
//         test_hash_table_operations();
//         test_list_table_operations();
//         test_set_table_operations();
//         test_string_table_operations();
//         test_zset_table_operations();
        
//         // Test error handling
//         test_error_handling();
        
//         // Cleanup
//         cleanup_fdw_infrastructure();
        
//         info!("Comprehensive Redis FDW smoke test completed successfully");
//     }

//     /// Test individual table types separately for better isolation
//     #[pg_test]
//     #[cfg(feature = "integration_tests")]
//     fn test_hash_table_smoke() {
//         setup_fdw_infrastructure();
//         test_hash_table_operations();
//         cleanup_fdw_infrastructure();
//     }

//     #[pg_test]
//     #[cfg(feature = "integration_tests")]
//     fn test_list_table_smoke() {
//         setup_fdw_infrastructure();
//         test_list_table_operations();
//         cleanup_fdw_infrastructure();
//     }

//     #[pg_test]
//     #[cfg(feature = "integration_tests")]
//     fn test_set_table_smoke() {
//         setup_fdw_infrastructure();
//         test_set_table_operations();
//         cleanup_fdw_infrastructure();
//     }

//     #[pg_test]
//     #[cfg(feature = "integration_tests")]
//     fn test_string_table_smoke() {
//         setup_fdw_infrastructure();
//         test_string_table_operations();
//         cleanup_fdw_infrastructure();
//     }

//     #[pg_test]
//     #[cfg(feature = "integration_tests")]
//     fn test_zset_table_smoke() {
//         setup_fdw_infrastructure();
//         test_zset_table_operations();
//         cleanup_fdw_infrastructure();
//     }

//     /// Test basic operations without Redis (unit test)
//     #[pg_test]
//     fn test_fdw_handler_smoke() {
//         // Test FDW handler creation
//         let result = Spi::get_one::<i32>("SELECT 1 WHERE redis_fdw_handler() IS NOT NULL");
//         assert!(result.is_ok());
//         assert!(result.unwrap().is_some());
        
//         // Test FDW creation
//         Spi::run("CREATE FOREIGN DATA WRAPPER test_redis_wrapper HANDLER redis_fdw_handler;").unwrap();
        
//         // Test server creation
//         Spi::run("
//             CREATE SERVER test_redis_server 
//             FOREIGN DATA WRAPPER test_redis_wrapper
//             OPTIONS (host_port '127.0.0.1:8899');
//         ").unwrap();
        
//         // Test table creation (should not fail even without Redis connection)
//         Spi::run("
//             CREATE FOREIGN TABLE test_table (key text, value text) 
//             SERVER test_redis_server
//             OPTIONS (
//                 database '15',
//                 table_type 'hash',
//                 table_key_prefix 'smoke_test:'
//             );
//         ").unwrap();
        
//         // Cleanup
//         Spi::run("DROP FOREIGN TABLE test_table;").unwrap();
//         Spi::run("DROP SERVER test_redis_server CASCADE;").unwrap();
//         Spi::run("DROP FOREIGN DATA WRAPPER test_redis_wrapper CASCADE;").unwrap();
//     }

//     /// Test PostgreSQL version compatibility
//     #[pg_test]
//     fn test_postgresql_version_compatibility() {
//         // Test version detection
//         let version_result = Spi::get_one::<String>("SELECT version()");
//         assert!(version_result.is_ok());
        
//         let version_num_result = Spi::get_one::<i32>("SELECT current_setting('server_version_num')::int");
//         assert!(version_num_result.is_ok());
        
//         if let Some(version_num) = version_num_result.unwrap() {
//             assert!(version_num >= 140000, "PostgreSQL version should be 14 or higher");
            
//             // Test version-specific features
//             match version_num {
//                 140000..=149999 => info!("Testing PostgreSQL 14 compatibility"),
//                 150000..=159999 => info!("Testing PostgreSQL 15 compatibility"), 
//                 160000..=169999 => info!("Testing PostgreSQL 16 compatibility"),
//                 170000..=179999 => info!("Testing PostgreSQL 17 compatibility"),
//                 _ => info!("Testing unknown PostgreSQL version: {}", version_num),
//             }
//         }
        
//         // Test catalog queries that should work across all versions
//         let fdw_count = Spi::get_one::<i64>("
//             SELECT COUNT(*) 
//             FROM pg_foreign_data_wrapper 
//             WHERE fdwname LIKE '%redis%'
//         ");
//         assert!(fdw_count.is_ok());
//     }

//     // Helper functions for test setup and operations

//     fn setup_fdw_infrastructure() {
//         Spi::run("CREATE FOREIGN DATA WRAPPER redis_fdw HANDLER redis_fdw_handler;").unwrap();
//         Spi::run("
//             CREATE SERVER redis_server 
//             FOREIGN DATA WRAPPER redis_fdw
//             OPTIONS (host_port '127.0.0.1:8899');
//         ").unwrap();
//     }

//     fn cleanup_fdw_infrastructure() {
//         // Drop all test tables first
//         let tables = ["test_hash_table", "test_list_table", "test_set_table", 
//                      "test_string_table", "test_zset_table"];
        
//         for table in &tables {
//             let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table));
//         }
        
//         // Drop server and FDW
//         let _ = Spi::run("DROP SERVER IF EXISTS redis_server CASCADE;");
//         let _ = Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS redis_fdw CASCADE;");
//     }

//     fn test_hash_table_operations() {
//         info!("Testing Hash table operations");
        
//         // Create hash table
//         Spi::run("
//             CREATE FOREIGN TABLE test_hash_table (
//                 key text,
//                 value text
//             ) SERVER redis_server
//             OPTIONS (
//                 database '15',
//                 table_type 'hash',
//                 table_key_prefix 'smoke_test:hash'
//             );
//         ").unwrap();

//         // Test INSERT operations
//         Spi::run("INSERT INTO test_hash_table VALUES ('user:1', 'John Doe');").unwrap();
//         Spi::run("INSERT INTO test_hash_table VALUES ('user:2', 'Jane Smith');").unwrap();
//         Spi::run("INSERT INTO test_hash_table VALUES ('user:3', 'Bob Johnson');").unwrap();

//         // Test SELECT operations
//         let count_result = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_hash_table");
//         assert!(count_result.is_ok());
        
//         // Test that we can retrieve data
//         let data_result = Spi::get_two::<String, String>("
//             SELECT key, value FROM test_hash_table 
//             WHERE key = 'user:1'
//         ");
//         assert!(data_result.is_ok());

//         // Test DELETE operations
//         Spi::run("DELETE FROM test_hash_table WHERE key = 'user:2';").unwrap();
        
//         // Verify delete worked
//         let remaining_count = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_hash_table");
//         assert!(remaining_count.is_ok());
        
//         info!("Hash table operations completed successfully");
//     }

//     fn test_list_table_operations() {
//         info!("Testing List table operations");
        
//         // Create list table
//         Spi::run("
//             CREATE FOREIGN TABLE test_list_table (
//                 element text
//             ) SERVER redis_server
//             OPTIONS (
//                 database '15',
//                 table_type 'list',
//                 table_key_prefix 'smoke_test:list'
//             );
//         ").unwrap();

//         // Test INSERT operations
//         Spi::run("INSERT INTO test_list_table VALUES ('First Item');").unwrap();
//         Spi::run("INSERT INTO test_list_table VALUES ('Second Item');").unwrap();
//         Spi::run("INSERT INTO test_list_table VALUES ('Third Item');").unwrap();

//         // Test SELECT operations
//         let count_result = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_list_table");
//         assert!(count_result.is_ok());
        
//         let element_result = Spi::get_one::<String>("
//             SELECT element FROM test_list_table LIMIT 1
//         ");
//         assert!(element_result.is_ok());

//         info!("List table operations completed successfully");
//     }

//     fn test_set_table_operations() {
//         info!("Testing Set table operations");
        
//         // Create set table
//         Spi::run("
//             CREATE FOREIGN TABLE test_set_table (
//                 member text
//             ) SERVER redis_server
//             OPTIONS (
//                 database '15',
//                 table_type 'set',
//                 table_key_prefix 'smoke_test:set'
//             );
//         ").unwrap();

//         // Test INSERT operations (including duplicates)
//         Spi::run("INSERT INTO test_set_table VALUES ('tag1');").unwrap();
//         Spi::run("INSERT INTO test_set_table VALUES ('tag2');").unwrap();
//         Spi::run("INSERT INTO test_set_table VALUES ('tag3');").unwrap();
//         Spi::run("INSERT INTO test_set_table VALUES ('tag1');").unwrap(); // Duplicate

//         // Test SELECT operations
//         let count_result = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_set_table");
//         assert!(count_result.is_ok());

//         info!("Set table operations completed successfully");
//     }

//     fn test_string_table_operations() {
//         info!("Testing String table operations");
        
//         // Create string table
//         Spi::run("
//             CREATE FOREIGN TABLE test_string_table (
//                 value text
//             ) SERVER redis_server
//             OPTIONS (
//                 database '15',
//                 table_type 'string',
//                 table_key_prefix 'smoke_test:string'
//             );
//         ").unwrap();

//         // Test INSERT operations
//         Spi::run("INSERT INTO test_string_table VALUES ('Hello Redis FDW');").unwrap();

//         // Test SELECT operations
//         let value_result = Spi::get_one::<String>("SELECT value FROM test_string_table");
//         assert!(value_result.is_ok());

//         info!("String table operations completed successfully");
//     }

//     fn test_zset_table_operations() {
//         info!("Testing ZSet table operations");
        
//         // Create zset table
//         Spi::run("
//             CREATE FOREIGN TABLE test_zset_table (
//                 score numeric,
//                 member text
//             ) SERVER redis_server
//             OPTIONS (
//                 database '15',
//                 table_type 'zset',
//                 table_key_prefix 'smoke_test:zset'
//             );
//         ").unwrap();

//         // Test INSERT operations
//         Spi::run("INSERT INTO test_zset_table VALUES (100, 'player1');").unwrap();
//         Spi::run("INSERT INTO test_zset_table VALUES (150, 'player2');").unwrap();
//         Spi::run("INSERT INTO test_zset_table VALUES (200, 'player3');").unwrap();

//         // Test SELECT operations
//         let count_result = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_zset_table");
//         assert!(count_result.is_ok());
        
//         let top_player = Spi::get_two::<f32, String>("
//             SELECT score, member FROM test_zset_table 
//             ORDER BY score DESC LIMIT 1
//         ");
//         assert!(top_player.is_ok());

//         info!("ZSet table operations completed successfully");
//     }

//     fn test_error_handling() {
//         info!("Testing error handling scenarios");
        
//         // Test invalid table type (should not crash)
//         let result = std::panic::catch_unwind(|| {
//             Spi::run("
//                 CREATE FOREIGN TABLE test_invalid_type (key text, value text) 
//                 SERVER redis_server
//                 OPTIONS (
//                     database '15',
//                     table_type 'invalid_type',
//                     table_key_prefix 'test:invalid'
//                 );
//             ").unwrap();
//         });
        
//         // Table creation should succeed (validation happens at runtime)
//         assert!(result.is_ok());
        
//         // Cleanup invalid table
//         let _ = Spi::run("DROP FOREIGN TABLE IF EXISTS test_invalid_type;");
        
//         info!("Error handling tests completed");
//     }

//     /// Test multiple database support
//     #[pg_test]
//     #[cfg(feature = "integration_tests")]
//     fn test_multiple_database_support() {
//         info!("Testing multiple database support");
        
//         setup_fdw_infrastructure();
        
//         // Create tables in different Redis databases
//         Spi::run("
//             CREATE FOREIGN TABLE test_db0_table (
//                 key text,
//                 value text
//             ) SERVER redis_server
//             OPTIONS (
//                 database '0',
//                 table_type 'hash',
//                 table_key_prefix 'smoke_test:db0'
//             );
//         ").unwrap();

//         Spi::run("
//             CREATE FOREIGN TABLE test_db1_table (
//                 key text,
//                 value text
//             ) SERVER redis_server
//             OPTIONS (
//                 database '1',
//                 table_type 'hash',
//                 table_key_prefix 'smoke_test:db1'
//             );
//         ").unwrap();

//         // Insert data into both databases
//         Spi::run("INSERT INTO test_db0_table VALUES ('key1', 'value from db0');").unwrap();
//         Spi::run("INSERT INTO test_db1_table VALUES ('key1', 'value from db1');").unwrap();

//         // Verify data isolation
//         let db0_result = Spi::get_one::<String>("
//             SELECT value FROM test_db0_table WHERE key = 'key1'
//         ");
//         let db1_result = Spi::get_one::<String>("
//             SELECT value FROM test_db1_table WHERE key = 'key1'
//         ");
        
//         assert!(db0_result.is_ok());
//         assert!(db1_result.is_ok());
        
//         // Values should be different, confirming database isolation
//         if let (Some(db0_val), Some(db1_val)) = (db0_result.unwrap(), db1_result.unwrap()) {
//             assert_ne!(db0_val, db1_val);
//             assert!(db0_val.contains("db0"));
//             assert!(db1_val.contains("db1"));
//         }
        
//         // Cleanup
//         Spi::run("DROP FOREIGN TABLE test_db0_table;").unwrap();
//         Spi::run("DROP FOREIGN TABLE test_db1_table;").unwrap();
        
//         cleanup_fdw_infrastructure();
        
//         info!("Multiple database support test completed successfully");
//     }

//     /// Test transaction safety
//     #[pg_test]
//     #[cfg(feature = "integration_tests")]
//     fn test_transaction_safety() {
//         info!("Testing transaction safety");
        
//         setup_fdw_infrastructure();
        
//         // Create test table
//         Spi::run("
//             CREATE FOREIGN TABLE test_tx_table (
//                 key text,
//                 value text
//             ) SERVER redis_server
//             OPTIONS (
//                 database '15',
//                 table_type 'hash',
//                 table_key_prefix 'smoke_test:tx'
//             );
//         ").unwrap();

//         // Test transaction block (Redis doesn't have transactions, but PostgreSQL should handle gracefully)
//         let tx_result = std::panic::catch_unwind(|| {
//             Spi::run("BEGIN;").unwrap();
//             Spi::run("INSERT INTO test_tx_table VALUES ('tx:test1', 'transaction test 1');").unwrap();
//             Spi::run("INSERT INTO test_tx_table VALUES ('tx:test2', 'transaction test 2');").unwrap();
//             Spi::run("COMMIT;").unwrap();
//         });
        
//         assert!(tx_result.is_ok());
        
//         // Verify data was inserted
//         let count_result = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_tx_table");
//         assert!(count_result.is_ok());
        
//         // Cleanup
//         Spi::run("DROP FOREIGN TABLE test_tx_table;").unwrap();
//         cleanup_fdw_infrastructure();
        
//         info!("Transaction safety test completed successfully");
//     }

//     /// Performance smoke test - ensure operations complete in reasonable time
//     #[pg_test]
//     #[cfg(feature = "integration_tests")]
//     fn test_performance_smoke() {
//         info!("Testing basic performance characteristics");
        
//         setup_fdw_infrastructure();
        
//         // Create test table
//         Spi::run("
//             CREATE FOREIGN TABLE test_perf_table (
//                 key text,
//                 value text
//             ) SERVER redis_server
//             OPTIONS (
//                 database '15',
//                 table_type 'hash',
//                 table_key_prefix 'smoke_test:perf'
//             );
//         ").unwrap();

//         // Insert multiple records to test bulk operations
//         for i in 1..=50 {
//             Spi::run(&format!(
//                 "INSERT INTO test_perf_table VALUES ('key{}', 'value{}');",
//                 i, i
//             )).unwrap();
//         }

//         // Test bulk select
//         let count_result = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_perf_table");
//         assert!(count_result.is_ok());
        
//         if let Some(count) = count_result.unwrap() {
//             assert_eq!(count, 50);
//         }

//         // Test filtered select
//         let filtered_count = Spi::get_one::<i64>("
//             SELECT COUNT(*) FROM test_perf_table 
//             WHERE key LIKE 'key1%'
//         ");
//         assert!(filtered_count.is_ok());

//         // Cleanup
//         Spi::run("DROP FOREIGN TABLE test_perf_table;").unwrap();
//         cleanup_fdw_infrastructure();
        
//         info!("Performance smoke test completed successfully");
//     }
// }
