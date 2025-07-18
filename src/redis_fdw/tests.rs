#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema] 
mod tests {
    use pgrx::prelude::*;
    use crate::redis_fdw::{
        tables::{RedisHashTable, RedisListTable},
        state::{RedisFdwState, RedisTableType}
    };
    use std::ptr;

    #[pg_test]
    fn test_redis_fdw_handler_creation() {
        // Test that the FDW handler function exists and can be called
        let result = Spi::get_one::<i32>("SELECT 1 WHERE redis_fdw_handler() IS NOT NULL");
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }


    #[pg_test]
    fn test_foreign_table_with_missing_options() {
        // Setup
        Spi::run("CREATE FOREIGN DATA WRAPPER test_redis_wrapper HANDLER redis_fdw_handler;").unwrap();
        Spi::run("
            CREATE SERVER test_redis_server 
            FOREIGN DATA WRAPPER test_redis_wrapper
            OPTIONS (host_port '127.0.0.1:8899');
        ").unwrap();
        
        // Test creating a table with missing required options should work (validation happens at runtime)
        let result = std::panic::catch_unwind(|| {
            Spi::run("
                CREATE FOREIGN TABLE test_invalid_table (key text, value text) 
                SERVER test_redis_server
                OPTIONS (database '0');
            ").unwrap();
        });
        
        // Table creation should succeed, but runtime operations would fail
        assert!(result.is_ok());
        
        // Clean up
        Spi::run("DROP FOREIGN TABLE IF EXISTS test_invalid_table;").unwrap();
        Spi::run("DROP SERVER test_redis_server CASCADE;").unwrap();
        Spi::run("DROP FOREIGN DATA WRAPPER test_redis_wrapper CASCADE;").unwrap();
    }

    // State management tests

    #[pg_test]
    fn test_redis_fdw_state_creation() {
        // Test creating a new FDW state
        let state = RedisFdwState::new(ptr::null_mut());
        assert_eq!(state.database, 0);
        assert_eq!(state.host_port, "");
        assert_eq!(state.table_key_prefix, "");
        assert_eq!(state.row_count, 0);
        assert!(matches!(state.table_type, RedisTableType::None));
        assert!(state.redis_connection.is_none());
    }

    #[pg_test]
    fn test_redis_fdw_state_update_from_options() {
        use std::collections::HashMap;
        
        let mut state = RedisFdwState::new(ptr::null_mut());
        let mut options = HashMap::new();
        
        // Test with required options
        options.insert("host_port".to_string(), "127.0.0.1:8899".to_string());
        options.insert("table_type".to_string(), "hash".to_string());
        options.insert("table_key_prefix".to_string(), "test:".to_string());
        options.insert("database".to_string(), "1".to_string());
        
        state.update_from_options(options);
        
        assert_eq!(state.host_port, "127.0.0.1:8899");
        assert_eq!(state.table_key_prefix, "test:");
        assert_eq!(state.database, 1);
    }

    #[pg_test]
    fn test_redis_table_type_data_len() {
        // Test data_len for different table types
        let mut state = RedisFdwState::new(ptr::null_mut());
        
        // Test None type
        assert_eq!(state.data_len(), 0);
        
        // Test Hash type with data
        let mut hash_table = RedisHashTable::new();
        hash_table.data = vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ];
        state.table_type = RedisTableType::Hash(hash_table);
        assert_eq!(state.data_len(), 2);
        
        // Test List type with data  
        let mut list_table = RedisListTable::new();
        list_table.data = vec!["item1".to_string(), "item2".to_string(), "item3".to_string()];
        state.table_type = RedisTableType::List(list_table);
        assert_eq!(state.data_len(), 3);
        
        // Test empty collections
        state.table_type = RedisTableType::Hash(RedisHashTable::new());
        assert_eq!(state.data_len(), 0);
        
        state.table_type = RedisTableType::List(RedisListTable::new());
        assert_eq!(state.data_len(), 0);
    }

    #[pg_test]
    fn test_redis_fdw_state_is_read_end() {
        let mut state = RedisFdwState::new(ptr::null_mut());
        
        // Test with no data
        assert!(state.is_read_end());
        
        // Test with hash data
        let mut hash_table = RedisHashTable::new();
        hash_table.data = vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ];
        state.table_type = RedisTableType::Hash(hash_table);
        state.row_count = 0;
        assert!(!state.is_read_end());
        
        state.row_count = 1;
        assert!(!state.is_read_end());
        
        state.row_count = 2;
        assert!(state.is_read_end());
        
        state.row_count = 3;
        assert!(state.is_read_end());
    }

    #[pg_test]
    fn test_foreign_table_with_different_databases() {
        // Setup
        Spi::run("CREATE FOREIGN DATA WRAPPER test_redis_wrapper HANDLER redis_fdw_handler;").unwrap();
        Spi::run("
            CREATE SERVER test_redis_server 
            FOREIGN DATA WRAPPER test_redis_wrapper
            OPTIONS (host_port '127.0.0.1:8899');
        ").unwrap();
        
        // Test tables with different database numbers
        Spi::run("
            CREATE FOREIGN TABLE test_db0_table (key text, value text) 
            SERVER test_redis_server
            OPTIONS (
                database '0',
                table_type 'hash',
                table_key_prefix 'test:db0:'
            );
        ").unwrap();
        
        Spi::run("
            CREATE FOREIGN TABLE test_db1_table (key text, value text) 
            SERVER test_redis_server
            OPTIONS (
                database '1',
                table_type 'hash',
                table_key_prefix 'test:db1:'
            );
        ").unwrap();
        
        // Verify both tables were created
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) 
             FROM pg_class c 
             JOIN pg_foreign_table ft ON c.oid = ft.ftrelid 
             WHERE c.relname LIKE 'test_db%_table'"
        );
        assert!(count.is_ok());
        assert_eq!(count.unwrap(), Some(2));
        
        // Clean up
        Spi::run("DROP FOREIGN TABLE test_db0_table;").unwrap();
        Spi::run("DROP FOREIGN TABLE test_db1_table;").unwrap();
        Spi::run("DROP SERVER test_redis_server CASCADE;").unwrap();
        Spi::run("DROP FOREIGN DATA WRAPPER test_redis_wrapper CASCADE;").unwrap();
    }

    #[pg_test]
    fn test_foreign_table_options_validation() {
        // Setup
        Spi::run("CREATE FOREIGN DATA WRAPPER test_redis_wrapper HANDLER redis_fdw_handler;").unwrap();
        
        // Test server without host_port option - should work but fail at runtime
        let result = std::panic::catch_unwind(|| {
            Spi::run("
                CREATE SERVER test_invalid_server 
                FOREIGN DATA WRAPPER test_redis_wrapper;
            ").unwrap();
        });
        assert!(result.is_ok()); // Server creation should succeed
        
        // Clean up
        Spi::run("DROP SERVER IF EXISTS test_invalid_server CASCADE;").unwrap();
        Spi::run("DROP FOREIGN DATA WRAPPER test_redis_wrapper CASCADE;").unwrap();
    }

    // Integration tests (require Redis server running)
    // These tests are marked with a special feature flag to run only when Redis is available
    
    #[pg_test]
    #[cfg(feature = "integration_tests")]
    fn test_redis_connection_and_select() {
        // Setup Redis FDW
        Spi::run("CREATE FOREIGN DATA WRAPPER redis_wrapper HANDLER redis_fdw_handler;").unwrap();
        Spi::run("
            CREATE SERVER redis_server 
            FOREIGN DATA WRAPPER redis_wrapper
            OPTIONS (host_port '127.0.0.1:8899');
        ").unwrap();
        
        // Create hash table
        Spi::run("
            CREATE FOREIGN TABLE test_redis_hash (key text, value text) 
            SERVER redis_server
            OPTIONS (
                database '15',
                table_type 'hash',
                table_key_prefix 'fdw_test:hash'
            );
        ").unwrap();
        
        // Test SELECT (should work even with empty Redis hash)
        let result = std::panic::catch_unwind(|| {
            let _ = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_redis_hash");
        });
        
        // Should not panic, even if Redis is empty
        assert!(result.is_ok());
        
        // Clean up
        Spi::run("DROP FOREIGN TABLE test_redis_hash;").unwrap();
        Spi::run("DROP SERVER redis_server CASCADE;").unwrap();
        Spi::run("DROP FOREIGN DATA WRAPPER redis_wrapper CASCADE;").unwrap();
    }

    #[pg_test]
    #[cfg(feature = "integration_tests")]
    fn test_redis_hash_insert() {
        // Setup Redis FDW
        Spi::run("CREATE FOREIGN DATA WRAPPER redis_wrapper HANDLER redis_fdw_handler;").unwrap();
        Spi::run("
            CREATE SERVER redis_server 
            FOREIGN DATA WRAPPER redis_wrapper
            OPTIONS (host_port '127.0.0.1:8899');
        ").unwrap();
        
        // Create hash table
        Spi::run("
            CREATE FOREIGN TABLE test_insert_hash (key text, value text) 
            SERVER redis_server
            OPTIONS (
                database '15',
                table_type 'hash',
                table_key_prefix 'fdw_test:insert_hash'
            );
        ").unwrap();
        
        // Test INSERT
        let result = std::panic::catch_unwind(|| {
            Spi::run("INSERT INTO test_insert_hash VALUES ('test_key', 'test_value');").unwrap();
        });
        
        assert!(result.is_ok());
        
        // Clean up
        Spi::run("DROP FOREIGN TABLE test_insert_hash;").unwrap();
        Spi::run("DROP SERVER redis_server CASCADE;").unwrap();
        Spi::run("DROP FOREIGN DATA WRAPPER redis_wrapper CASCADE;").unwrap();
    }

    #[pg_test]
    #[cfg(feature = "integration_tests")]
    fn test_redis_list_operations() {
        // Setup Redis FDW
        Spi::run("CREATE FOREIGN DATA WRAPPER redis_wrapper HANDLER redis_fdw_handler;").unwrap();
        Spi::run("
            CREATE SERVER redis_server 
            FOREIGN DATA WRAPPER redis_wrapper
            OPTIONS (host_port '127.0.0.1:8899');
        ").unwrap();
        
        // Create list table
        Spi::run("
            CREATE FOREIGN TABLE test_list (element text) 
            SERVER redis_server
            OPTIONS (
                database '15',
                table_type 'list',
                table_key_prefix 'fdw_test:list'
            );
        ").unwrap();
        
        // Test INSERT to list
        let result = std::panic::catch_unwind(|| {
            Spi::run("INSERT INTO test_list VALUES ('item1');").unwrap();
            Spi::run("INSERT INTO test_list VALUES ('item2');").unwrap();
        });
        
        assert!(result.is_ok());
        
        // Test SELECT from list
        let select_result = std::panic::catch_unwind(|| {
            let _ = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_list");
        });
        
        assert!(select_result.is_ok());
        
        // Clean up
        Spi::run("DROP FOREIGN TABLE test_list;").unwrap();
        Spi::run("DROP SERVER redis_server CASCADE;").unwrap();
        Spi::run("DROP FOREIGN DATA WRAPPER redis_wrapper CASCADE;").unwrap();
    }

    // #[pg_test]
    // fn test_update_and_delete_operations() {
    //     // Test that UPDATE and DELETE don't crash (even though they're not implemented)
    //     Spi::run("CREATE FOREIGN DATA WRAPPER redis_wrapper HANDLER redis_fdw_handler;").unwrap();
    //     Spi::run("
    //         CREATE SERVER redis_server 
    //         FOREIGN DATA WRAPPER redis_wrapper
    //         OPTIONS (host_port '127.0.0.1:8899');
    //     ").unwrap();
        
    //     Spi::run("
    //         CREATE FOREIGN TABLE test_update_delete (key text, value text) 
    //         SERVER redis_server
    //         OPTIONS (
    //             database '0',
    //             table_type 'hash',
    //             table_key_prefix 'test:'
    //         );
    //     ").unwrap();
        
    //     // These should not crash, even though they don't actually do anything
    //     let update_result = std::panic::catch_unwind(|| {
    //         Spi::run("UPDATE test_update_delete SET value = 'new_value' WHERE key = 'some_key';").unwrap();
    //     });
        
    //     let delete_result = std::panic::catch_unwind(|| {
    //         Spi::run("DELETE FROM test_update_delete WHERE key = 'some_key';").unwrap();
    //     });
        
    //     assert!(update_result.is_ok());
    //     assert!(delete_result.is_ok());
        
    //     // Clean up
    //     Spi::run("DROP FOREIGN TABLE test_update_delete;").unwrap();
    //     Spi::run("DROP SERVER redis_server CASCADE;").unwrap();
    //     Spi::run("DROP FOREIGN DATA WRAPPER redis_wrapper CASCADE;").unwrap();
    // }
}



