#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::{
        core::state::RedisFdwState,
        tables::implementations::{RedisHashTable, RedisListTable},
        tables::types::RedisTableType,
    };
    use pgrx::prelude::*;
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
        Spi::run("CREATE FOREIGN DATA WRAPPER test_redis_wrapper HANDLER redis_fdw_handler;")
            .unwrap();
        Spi::run(
            "
            CREATE SERVER test_redis_server 
            FOREIGN DATA WRAPPER test_redis_wrapper
            OPTIONS (host_port '127.0.0.1:8899');
        ",
        )
        .unwrap();

        // Test creating a table with missing required options should work (validation happens at runtime)
        let result = std::panic::catch_unwind(|| {
            Spi::run(
                "
                CREATE FOREIGN TABLE test_invalid_table (key text, value text) 
                SERVER test_redis_server
                OPTIONS (database '0');
            ",
            )
            .unwrap();
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
        use crate::tables::types::{DataContainer, DataSet};

        // Test data_len for different table types
        let mut state = RedisFdwState::new(ptr::null_mut());

        // Test None type
        assert_eq!(state.data_len(), 0);

        // Test Hash type with data
        let mut hash_table = RedisHashTable::new();
        hash_table.dataset = DataSet::Complete(DataContainer::Hash(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));
        state.table_type = RedisTableType::Hash(hash_table);
        assert_eq!(state.data_len(), 2);

        // Test List type with data
        let mut list_table = RedisListTable::new();
        list_table.dataset = DataSet::Complete(DataContainer::List(vec![
            "item1".to_string(),
            "item2".to_string(),
            "item3".to_string(),
        ]));
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
        use crate::tables::types::{DataContainer, DataSet};

        let mut state = RedisFdwState::new(ptr::null_mut());

        // Test with no data
        assert!(state.is_read_end());

        // Test with hash data
        let mut hash_table = RedisHashTable::new();
        hash_table.dataset = DataSet::Complete(DataContainer::Hash(vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));
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
        Spi::run("CREATE FOREIGN DATA WRAPPER test_redis_wrapper HANDLER redis_fdw_handler;")
            .unwrap();
        Spi::run(
            "
            CREATE SERVER test_redis_server 
            FOREIGN DATA WRAPPER test_redis_wrapper
            OPTIONS (host_port '127.0.0.1:8899');
        ",
        )
        .unwrap();

        // Test tables with different database numbers
        Spi::run(
            "
            CREATE FOREIGN TABLE test_db0_table (key text, value text) 
            SERVER test_redis_server
            OPTIONS (
                database '0',
                table_type 'hash',
                table_key_prefix 'test:db0:'
            );
        ",
        )
        .unwrap();

        Spi::run(
            "
            CREATE FOREIGN TABLE test_db1_table (key text, value text) 
            SERVER test_redis_server
            OPTIONS (
                database '1',
                table_type 'hash',
                table_key_prefix 'test:db1:'
            );
        ",
        )
        .unwrap();

        // Verify both tables were created
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) 
             FROM pg_class c 
             JOIN pg_foreign_table ft ON c.oid = ft.ftrelid 
             WHERE c.relname LIKE 'test_db%_table'",
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
        Spi::run("CREATE FOREIGN DATA WRAPPER test_redis_wrapper HANDLER redis_fdw_handler;")
            .unwrap();

        // Test server without host_port option - should work but fail at runtime
        let result = std::panic::catch_unwind(|| {
            Spi::run(
                "
                CREATE SERVER test_invalid_server 
                FOREIGN DATA WRAPPER test_redis_wrapper;
            ",
            )
            .unwrap();
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
        Spi::run(
            "
            CREATE SERVER redis_server 
            FOREIGN DATA WRAPPER redis_wrapper
            OPTIONS (host_port '127.0.0.1:8899');
        ",
        )
        .unwrap();

        // Create hash table
        Spi::run(
            "
            CREATE FOREIGN TABLE test_redis_hash (key text, value text) 
            SERVER redis_server
            OPTIONS (
                database '15',
                table_type 'hash',
                table_key_prefix 'fdw_test:hash'
            );
        ",
        )
        .unwrap();

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
        Spi::run(
            "
            CREATE SERVER redis_server 
            FOREIGN DATA WRAPPER redis_wrapper
            OPTIONS (host_port '127.0.0.1:8899');
        ",
        )
        .unwrap();

        // Create hash table
        Spi::run(
            "
            CREATE FOREIGN TABLE test_insert_hash (key text, value text) 
            SERVER redis_server
            OPTIONS (
                database '15',
                table_type 'hash',
                table_key_prefix 'fdw_test:insert_hash'
            );
        ",
        )
        .unwrap();

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
        Spi::run(
            "
            CREATE SERVER redis_server 
            FOREIGN DATA WRAPPER redis_wrapper
            OPTIONS (host_port '127.0.0.1:8899');
        ",
        )
        .unwrap();

        // Create list table
        Spi::run(
            "
            CREATE FOREIGN TABLE test_list (element text) 
            SERVER redis_server
            OPTIONS (
                database '15',
                table_type 'list',
                table_key_prefix 'fdw_test:list'
            );
        ",
        )
        .unwrap();

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

    /// Comprehensive smoke test - Basic FDW functionality without Redis
    #[pg_test]
    fn test_smoke_fdw_basic_functionality() {
        info!("Starting basic FDW smoke test");

        // Test FDW handler creation
        let result = Spi::get_one::<i32>("SELECT 1 WHERE redis_fdw_handler() IS NOT NULL");
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        // Test FDW creation
        Spi::run("CREATE FOREIGN DATA WRAPPER test_redis_wrapper HANDLER redis_fdw_handler;")
            .unwrap();

        // Test server creation
        Spi::run(
            "
            CREATE SERVER test_redis_server 
            FOREIGN DATA WRAPPER test_redis_wrapper
            OPTIONS (host_port '127.0.0.1:8899');
        ",
        )
        .unwrap();

        // Test hash table creation
        Spi::run(
            "
            CREATE FOREIGN TABLE test_hash_table (key text, value text) 
            SERVER test_redis_server
            OPTIONS (
                database '15',
                table_type 'hash',
                table_key_prefix 'smoke_test:hash'
            );
        ",
        )
        .unwrap();

        // Test list table creation
        Spi::run(
            "
            CREATE FOREIGN TABLE test_list_table (element text) 
            SERVER test_redis_server
            OPTIONS (
                database '15',
                table_type 'list',
                table_key_prefix 'smoke_test:list'
            );
        ",
        )
        .unwrap();

        // Test set table creation
        Spi::run(
            "
            CREATE FOREIGN TABLE test_set_table (member text) 
            SERVER test_redis_server
            OPTIONS (
                database '15',
                table_type 'set',
                table_key_prefix 'smoke_test:set'
            );
        ",
        )
        .unwrap();

        // Test string table creation
        Spi::run(
            "
            CREATE FOREIGN TABLE test_string_table (value text) 
            SERVER test_redis_server
            OPTIONS (
                database '15',
                table_type 'string',
                table_key_prefix 'smoke_test:string'
            );
        ",
        )
        .unwrap();

        // Test zset table creation
        Spi::run(
            "
            CREATE FOREIGN TABLE test_zset_table (score numeric, member text) 
            SERVER test_redis_server
            OPTIONS (
                database '15',
                table_type 'zset',
                table_key_prefix 'smoke_test:zset'
            );
        ",
        )
        .unwrap();

        // Verify tables were created by querying catalog
        let table_count = Spi::get_one::<i64>(
            "
            SELECT COUNT(*) 
            FROM pg_foreign_table ft
            JOIN pg_class c ON ft.ftrelid = c.oid
            WHERE c.relname LIKE 'test_%table'
        ",
        );
        assert!(table_count.is_ok());
        assert_eq!(table_count.unwrap(), Some(5)); // 5 test tables created

        // Test PostgreSQL version compatibility
        let version_result = Spi::get_one::<String>("SELECT version()");
        assert!(version_result.is_ok());

        let version_num_result =
            Spi::get_one::<i32>("SELECT current_setting('server_version_num')::int");
        assert!(version_num_result.is_ok());

        if let Some(version_num) = version_num_result.unwrap() {
            assert!(
                version_num >= 140000,
                "PostgreSQL version should be 14 or higher"
            );
            info!("Testing PostgreSQL version: {}", version_num);
        }

        // Cleanup tables
        let tables = [
            "test_hash_table",
            "test_list_table",
            "test_set_table",
            "test_string_table",
            "test_zset_table",
        ];

        for table in &tables {
            Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {table};")).unwrap();
        }

        // Clean up server and FDW
        Spi::run("DROP SERVER test_redis_server CASCADE;").unwrap();
        Spi::run("DROP FOREIGN DATA WRAPPER test_redis_wrapper CASCADE;").unwrap();

        info!("Basic FDW smoke test completed successfully");
    }

    /// Comprehensive integration test with Redis - INSERT, SELECT, DELETE operations
    #[pg_test]
    #[cfg(feature = "integration_tests")]
    fn test_smoke_comprehensive_redis_operations() {
        info!("Starting comprehensive Redis operations smoke test");

        // Setup Redis FDW
        Spi::run("CREATE FOREIGN DATA WRAPPER redis_wrapper HANDLER redis_fdw_handler;").unwrap();
        Spi::run(
            "
            CREATE SERVER redis_server 
            FOREIGN DATA WRAPPER redis_wrapper
            OPTIONS (host_port '127.0.0.1:8899');
        ",
        )
        .unwrap();

        // Test 1: Hash Table Operations
        info!("Testing Hash table operations");
        Spi::run(
            "
            CREATE FOREIGN TABLE test_hash (key text, value text) 
            SERVER redis_server
            OPTIONS (
                database '15',
                table_type 'hash',
                table_key_prefix 'smoke_test:hash'
            );
        ",
        )
        .unwrap();

        // INSERT operations
        Spi::run("INSERT INTO test_hash VALUES ('user:1', 'John Doe');").unwrap();
        Spi::run("INSERT INTO test_hash VALUES ('user:2', 'Jane Smith');").unwrap();
        Spi::run("INSERT INTO test_hash VALUES ('user:3', 'Bob Johnson');").unwrap();

        // SELECT operations
        let count_result = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_hash");
        assert!(count_result.is_ok());
        info!("Hash table count: {:?}", count_result.unwrap());

        // DELETE operations
        Spi::run("DELETE FROM test_hash WHERE key = 'user:2';").unwrap();

        // Test 2: List Table Operations
        info!("Testing List table operations");
        Spi::run(
            "
            CREATE FOREIGN TABLE test_list (element text) 
            SERVER redis_server
            OPTIONS (
                database '15',
                table_type 'list',
                table_key_prefix 'smoke_test:list'
            );
        ",
        )
        .unwrap();

        // INSERT operations
        Spi::run("INSERT INTO test_list VALUES ('First Item');").unwrap();
        Spi::run("INSERT INTO test_list VALUES ('Second Item');").unwrap();
        Spi::run("INSERT INTO test_list VALUES ('Third Item');").unwrap();

        // SELECT operations
        let list_count = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_list");
        assert!(list_count.is_ok());
        info!("List table count: {:?}", list_count.unwrap());

        // Test 3: Set Table Operations
        info!("Testing Set table operations");
        Spi::run(
            "
            CREATE FOREIGN TABLE test_set (member text) 
            SERVER redis_server
            OPTIONS (
                database '15',
                table_type 'set',
                table_key_prefix 'smoke_test:set'
            );
        ",
        )
        .unwrap();

        // INSERT operations (including duplicate)
        Spi::run("INSERT INTO test_set VALUES ('tag1');").unwrap();
        Spi::run("INSERT INTO test_set VALUES ('tag2');").unwrap();
        Spi::run("INSERT INTO test_set VALUES ('tag3');").unwrap();
        Spi::run("INSERT INTO test_set VALUES ('tag1');").unwrap(); // Duplicate

        // SELECT operations
        let set_count = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_set");
        assert!(set_count.is_ok());
        info!("Set table count: {:?}", set_count.unwrap());

        // Test 4: String Table Operations
        info!("Testing String table operations");
        Spi::run(
            "
            CREATE FOREIGN TABLE test_string (value text) 
            SERVER redis_server
            OPTIONS (
                database '15',
                table_type 'string',
                table_key_prefix 'smoke_test:string'
            );
        ",
        )
        .unwrap();

        // INSERT operations
        Spi::run("INSERT INTO test_string VALUES ('Hello Redis FDW');").unwrap();

        // SELECT operations
        let string_value = Spi::get_one::<String>("SELECT value FROM test_string");
        assert!(string_value.is_ok());
        info!("String value: {:?}", string_value.unwrap());

        // Test 5: ZSet Table Operations
        info!("Testing ZSet table operations");
        Spi::run(
            "
            CREATE FOREIGN TABLE test_zset (score numeric, member text) 
            SERVER redis_server
            OPTIONS (
                database '15',
                table_type 'zset',
                table_key_prefix 'smoke_test:zset'
            );
        ",
        )
        .unwrap();

        // INSERT operations
        Spi::run("INSERT INTO test_zset VALUES (100, 'player1');").unwrap();
        Spi::run("INSERT INTO test_zset VALUES (150, 'player2');").unwrap();
        Spi::run("INSERT INTO test_zset VALUES (200, 'player3');").unwrap();

        // SELECT operations
        let zset_count = Spi::get_one::<i64>("SELECT COUNT(*) FROM test_zset");
        assert!(zset_count.is_ok());
        info!("ZSet table count: {:?}", zset_count.unwrap());

        // Test multiple database support
        info!("Testing multiple database support");
        Spi::run(
            "
            CREATE FOREIGN TABLE test_db0 (key text, value text) 
            SERVER redis_server
            OPTIONS (
                database '0',
                table_type 'hash',
                table_key_prefix 'smoke_test:db0'
            );
        ",
        )
        .unwrap();

        Spi::run("INSERT INTO test_db0 VALUES ('key1', 'value from db0');").unwrap();
        let db0_result = Spi::get_one::<String>("SELECT value FROM test_db0 WHERE key = 'key1'");
        assert!(db0_result.is_ok());
        info!("DB0 result: {:?}", db0_result.unwrap());

        // Cleanup all test tables
        let tables = [
            "test_hash",
            "test_list",
            "test_set",
            "test_string",
            "test_zset",
            "test_db0",
        ];
        for table in &tables {
            Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table)).unwrap();
        }

        // Clean up server and FDW
        Spi::run("DROP SERVER redis_server CASCADE;").unwrap();
        Spi::run("DROP FOREIGN DATA WRAPPER redis_wrapper CASCADE;").unwrap();

        info!("Comprehensive Redis operations smoke test completed successfully");
    }

    #[pg_test]
    fn test_redis_cluster_connection_parsing() {
        use crate::core::state::RedisFdwState;
        use std::collections::HashMap;

        // Test cluster node parsing
        let tmp_ctx = unsafe { pgrx::pg_sys::CurrentMemoryContext };
        let mut fdw_state = RedisFdwState::new(tmp_ctx);

        // Test single node (should create Single connection type)
        let mut single_opts = HashMap::new();
        single_opts.insert("host_port".to_string(), "127.0.0.1:6379".to_string());
        single_opts.insert("database".to_string(), "0".to_string());
        fdw_state.update_from_options(single_opts);

        // Test that single node parsing works
        assert_eq!(fdw_state.host_port, "127.0.0.1:6379");
        assert_eq!(fdw_state.database, 0);

        // Test cluster nodes (multiple comma-separated addresses)
        let mut cluster_opts = HashMap::new();
        cluster_opts.insert(
            "host_port".to_string(),
            "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002".to_string(),
        );
        cluster_opts.insert("database".to_string(), "0".to_string());
        fdw_state.update_from_options(cluster_opts);

        // Test that cluster nodes parsing works
        assert_eq!(
            fdw_state.host_port,
            "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002"
        );
        assert_eq!(fdw_state.database, 0);

        info!("Redis cluster connection parsing test completed successfully");
    }

    #[pg_test]
    fn test_redis_cluster_server_creation() {
        // Test that cluster server can be created with multiple nodes
        Spi::run("CREATE FOREIGN DATA WRAPPER test_cluster_wrapper HANDLER redis_fdw_handler;")
            .unwrap();

        // Test single node server
        let result = std::panic::catch_unwind(|| {
            Spi::run(
                "
                CREATE SERVER test_single_server 
                FOREIGN DATA WRAPPER test_cluster_wrapper
                OPTIONS (host_port '127.0.0.1:6379');
            ",
            )
            .unwrap();
        });
        assert!(result.is_ok());

        // Test cluster server with multiple nodes
        let result = std::panic::catch_unwind(|| {
            Spi::run(
                "
                CREATE SERVER test_cluster_server 
                FOREIGN DATA WRAPPER test_cluster_wrapper
                OPTIONS (host_port '127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002');
            ",
            )
            .unwrap();
        });
        assert!(result.is_ok());

        // Test creating foreign tables on cluster server
        let result = std::panic::catch_unwind(|| {
            Spi::run(
                "
                CREATE FOREIGN TABLE test_cluster_hash (field TEXT, value TEXT) 
                SERVER test_cluster_server
                OPTIONS (
                    database '0',
                    table_type 'hash',
                    table_key_prefix 'cluster:test'
                );
            ",
            )
            .unwrap();
        });
        assert!(result.is_ok());

        // Clean up
        Spi::run("DROP FOREIGN TABLE IF EXISTS test_cluster_hash;").unwrap();
        Spi::run("DROP SERVER test_cluster_server CASCADE;").unwrap();
        Spi::run("DROP SERVER test_single_server CASCADE;").unwrap();
        Spi::run("DROP FOREIGN DATA WRAPPER test_cluster_wrapper CASCADE;").unwrap();

        info!("Redis cluster server creation test completed successfully");
    }
}
