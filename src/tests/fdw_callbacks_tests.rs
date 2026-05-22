/// Integration tests for FDW callback extensions:
/// - ExplainForeignScan / ExplainForeignModify
/// - ExecForeignBatchInsert / GetForeignModifyBatchSize
/// - ExecForeignTruncate
/// - ImportForeignSchema
///
/// Prerequisites:
///   - Redis server running on 127.0.0.1:8899
///   - Database 15 available for testing

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    const REDIS_HOST_PORT: &str = "127.0.0.1:8899";
    const TEST_DATABASE: &str = "15";
    const FDW_NAME: &str = "fdw_callbacks_test_fdw";
    const SERVER_NAME: &str = "fdw_callbacks_test_srv";

    fn setup() {
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {FDW_NAME} CASCADE;"
        ));
        Spi::run(&format!(
            "CREATE FOREIGN DATA WRAPPER {FDW_NAME} HANDLER redis_fdw_handler;"
        ))
        .unwrap();
        Spi::run(&format!(
            "CREATE SERVER {SERVER_NAME} FOREIGN DATA WRAPPER {FDW_NAME} \
             OPTIONS (host_port '{REDIS_HOST_PORT}');"
        ))
        .unwrap();
    }

    fn teardown() {
        let _ = Spi::run(&format!("DROP SERVER IF EXISTS {SERVER_NAME} CASCADE;"));
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {FDW_NAME} CASCADE;"
        ));
    }

    fn create_table(name: &str, cols: &str, ttype: &str, key: &str) {
        Spi::run(&format!(
            "CREATE FOREIGN TABLE {name} ({cols}) SERVER {SERVER_NAME} OPTIONS (\
                database '{TEST_DATABASE}', table_type '{ttype}', table_key_prefix '{key}');"
        ))
        .unwrap();
    }

    fn create_table_with_batch(name: &str, cols: &str, ttype: &str, key: &str, batch_size: u32) {
        Spi::run(&format!(
            "CREATE FOREIGN TABLE {name} ({cols}) SERVER {SERVER_NAME} OPTIONS (\
                database '{TEST_DATABASE}', table_type '{ttype}', \
                table_key_prefix '{key}', batch_size '{batch_size}');"
        ))
        .unwrap();
    }

    fn drop_table(name: &str) {
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {name};"));
    }

    fn count(table: &str) -> i64 {
        Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {table};"))
            .unwrap()
            .unwrap()
    }

    fn flush_key(key: &str) {
        Spi::run("SELECT redis_fdw_handler(); -- ensure extension loaded").ok();
        let client = redis::Client::open(format!("redis://{REDIS_HOST_PORT}")).unwrap();
        let mut conn = client.get_connection().unwrap();
        let _: () = redis::cmd("SELECT")
            .arg(TEST_DATABASE)
            .query(&mut conn)
            .unwrap();
        let _: () = redis::cmd("DEL").arg(key).query(&mut conn).unwrap();
    }

    fn flush_pattern(pattern: &str) {
        let client = redis::Client::open(format!("redis://{REDIS_HOST_PORT}")).unwrap();
        let mut conn = client.get_connection().unwrap();
        let _: () = redis::cmd("SELECT")
            .arg(TEST_DATABASE)
            .query(&mut conn)
            .unwrap();
        let keys: Vec<String> = redis::cmd("KEYS").arg(pattern).query(&mut conn).unwrap();
        for key in keys {
            let _: () = redis::cmd("DEL").arg(&key).query(&mut conn).unwrap();
        }
    }

    // ================================================
    // EXPLAIN TESTS
    // ================================================

    #[pg_test]
    fn test_explain_foreign_scan_hash() {
        setup();
        let table = "explain_scan_hash";
        flush_key("explain:scan:hash");
        create_table(table, "key text, value text", "hash", "explain:scan:hash");

        Spi::run(&format!("INSERT INTO {table} VALUES ('f1', 'v1');")).unwrap();

        let explain_output = Spi::get_one::<String>(&format!(
            "EXPLAIN (FORMAT TEXT, VERBOSE) SELECT * FROM {table};"
        ))
        .unwrap()
        .unwrap();

        assert!(
            explain_output.contains("Foreign Scan") || explain_output.contains("foreign"),
            "EXPLAIN should show foreign scan info"
        );

        drop_table(table);
        flush_key("explain:scan:hash");
        teardown();
    }

    #[pg_test]
    fn test_explain_analyze_foreign_scan() {
        setup();
        let table = "explain_analyze_scan";
        flush_key("explain:analyze:data");
        create_table(
            table,
            "key text, value text",
            "hash",
            "explain:analyze:data",
        );

        Spi::run(&format!("INSERT INTO {table} VALUES ('k1', 'v1');")).unwrap();
        Spi::run(&format!("INSERT INTO {table} VALUES ('k2', 'v2');")).unwrap();

        let explain_output = Spi::get_one::<String>(&format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT * FROM {table};"
        ))
        .unwrap()
        .unwrap();

        assert!(
            explain_output.contains("actual") || explain_output.contains("rows"),
            "EXPLAIN ANALYZE should show actual execution info"
        );

        drop_table(table);
        flush_key("explain:analyze:data");
        teardown();
    }

    #[pg_test]
    fn test_explain_foreign_modify() {
        setup();
        let table = "explain_modify_hash";
        flush_key("explain:modify:hash");
        create_table(table, "key text, value text", "hash", "explain:modify:hash");

        let explain_output = Spi::get_one::<String>(&format!(
            "EXPLAIN (FORMAT TEXT) INSERT INTO {table} VALUES ('k1', 'v1');"
        ))
        .unwrap()
        .unwrap();

        assert!(
            explain_output.contains("Insert") || explain_output.contains("Foreign"),
            "EXPLAIN INSERT should show modify plan"
        );

        drop_table(table);
        flush_key("explain:modify:hash");
        teardown();
    }

    // ================================================
    // BATCH INSERT TESTS
    // ================================================

    #[pg_test]
    fn test_batch_insert_hash() {
        setup();
        let table = "batch_ins_hash";
        flush_key("batch:ins:hash");
        create_table_with_batch(table, "key text, value text", "hash", "batch:ins:hash", 500);

        // Insert multiple rows - PG will batch them via ExecForeignBatchInsert
        Spi::run(&format!(
            "INSERT INTO {table} VALUES \
             ('f1', 'v1'), ('f2', 'v2'), ('f3', 'v3'), \
             ('f4', 'v4'), ('f5', 'v5');"
        ))
        .unwrap();

        let cnt = count(table);
        assert_eq!(cnt, 5, "Batch insert should produce 5 rows in hash");

        drop_table(table);
        flush_key("batch:ins:hash");
        teardown();
    }

    #[pg_test]
    fn test_batch_insert_list() {
        setup();
        let table = "batch_ins_list";
        flush_key("batch:ins:list");
        create_table_with_batch(table, "element text", "list", "batch:ins:list", 500);

        Spi::run(&format!(
            "INSERT INTO {table} VALUES ('a'), ('b'), ('c'), ('d'), ('e'), ('f');"
        ))
        .unwrap();

        let cnt = count(table);
        assert_eq!(cnt, 6, "Batch insert should produce 6 rows in list");

        drop_table(table);
        flush_key("batch:ins:list");
        teardown();
    }

    #[pg_test]
    fn test_batch_insert_set() {
        setup();
        let table = "batch_ins_set";
        flush_key("batch:ins:set");
        create_table_with_batch(table, "member text", "set", "batch:ins:set", 500);

        Spi::run(&format!(
            "INSERT INTO {table} VALUES ('m1'), ('m2'), ('m3'), ('m4');"
        ))
        .unwrap();

        let cnt = count(table);
        assert_eq!(cnt, 4, "Batch insert should produce 4 rows in set");

        drop_table(table);
        flush_key("batch:ins:set");
        teardown();
    }

    #[pg_test]
    fn test_batch_insert_zset() {
        setup();
        let table = "batch_ins_zset";
        flush_key("batch:ins:zset");
        create_table_with_batch(
            table,
            "member text, score text",
            "zset",
            "batch:ins:zset",
            500,
        );

        Spi::run(&format!(
            "INSERT INTO {table} VALUES ('a', '1.0'), ('b', '2.0'), ('c', '3.0');"
        ))
        .unwrap();

        let cnt = count(table);
        assert_eq!(cnt, 3, "Batch insert should produce 3 rows in zset");

        drop_table(table);
        flush_key("batch:ins:zset");
        teardown();
    }

    #[pg_test]
    fn test_batch_insert_string_multi_key() {
        setup();
        let table = "batch_ins_str_mk";
        flush_pattern("batchmk:str:*");
        create_table_with_batch(
            table,
            "key text, value text",
            "string",
            "batchmk:str:*",
            500,
        );

        Spi::run(&format!(
            "INSERT INTO {table} VALUES \
             ('batchmk:str:k1', 'v1'), ('batchmk:str:k2', 'v2'), ('batchmk:str:k3', 'v3');"
        ))
        .unwrap();

        let cnt = count(table);
        assert_eq!(
            cnt, 3,
            "Batch insert multi-key string should produce 3 rows"
        );

        drop_table(table);
        flush_pattern("batchmk:str:*");
        teardown();
    }

    // ================================================
    // TRUNCATE TESTS
    // ================================================

    #[pg_test]
    fn test_truncate_single_key_hash() {
        setup();
        let table = "trunc_hash";
        flush_key("trunc:hash:data");
        create_table(table, "key text, value text", "hash", "trunc:hash:data");

        Spi::run(&format!(
            "INSERT INTO {table} VALUES ('f1', 'v1'), ('f2', 'v2'), ('f3', 'v3');"
        ))
        .unwrap();

        let cnt = count(table);
        assert_eq!(cnt, 3, "Should have 3 rows before truncate");

        Spi::run(&format!("TRUNCATE {table};")).unwrap();

        let cnt_after = count(table);
        assert_eq!(cnt_after, 0, "Should have 0 rows after truncate");

        drop_table(table);
        teardown();
    }

    #[pg_test]
    fn test_truncate_single_key_list() {
        setup();
        let table = "trunc_list";
        flush_key("trunc:list:data");
        create_table(table, "element text", "list", "trunc:list:data");

        Spi::run(&format!(
            "INSERT INTO {table} VALUES ('a'), ('b'), ('c'), ('d');"
        ))
        .unwrap();

        let cnt = count(table);
        assert_eq!(cnt, 4, "Should have 4 rows before truncate");

        Spi::run(&format!("TRUNCATE {table};")).unwrap();

        let cnt_after = count(table);
        assert_eq!(cnt_after, 0, "Should have 0 rows after truncate");

        drop_table(table);
        teardown();
    }

    #[pg_test]
    fn test_truncate_multi_key_pattern() {
        setup();
        let table = "trunc_mk";
        flush_pattern("truncmk:*");
        create_table(table, "key text, value text", "string", "truncmk:*");

        Spi::run(&format!(
            "INSERT INTO {table} VALUES ('truncmk:k1', 'v1'), ('truncmk:k2', 'v2'), ('truncmk:k3', 'v3');"
        ))
        .unwrap();

        let cnt = count(table);
        assert_eq!(cnt, 3, "Should have 3 keys before truncate");

        Spi::run(&format!("TRUNCATE {table};")).unwrap();

        let cnt_after = count(table);
        assert_eq!(cnt_after, 0, "Should have 0 keys after truncate");

        drop_table(table);
        teardown();
    }

    #[pg_test]
    fn test_truncate_set() {
        setup();
        let table = "trunc_set";
        flush_key("trunc:set:data");
        create_table(table, "member text", "set", "trunc:set:data");

        Spi::run(&format!(
            "INSERT INTO {table} VALUES ('m1'), ('m2'), ('m3');"
        ))
        .unwrap();

        Spi::run(&format!("TRUNCATE {table};")).unwrap();

        let cnt_after = count(table);
        assert_eq!(cnt_after, 0, "Set should be empty after truncate");

        drop_table(table);
        teardown();
    }

    // ================================================
    // IMPORT FOREIGN SCHEMA TESTS
    // ================================================

    const IMPORT_SERVER_NAME: &str = "fdw_callbacks_import_srv";

    fn setup_import_server() {
        let _ = Spi::run(&format!(
            "DROP SERVER IF EXISTS {IMPORT_SERVER_NAME} CASCADE;"
        ));
        Spi::run(&format!(
            "CREATE SERVER {IMPORT_SERVER_NAME} FOREIGN DATA WRAPPER {FDW_NAME} \
             OPTIONS (host_port '{REDIS_HOST_PORT}', database '{TEST_DATABASE}');"
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_import_foreign_schema_basic() {
        setup();
        setup_import_server();
        flush_pattern("imptest:*");

        // Seed Redis with known keys
        let client = redis::Client::open(format!("redis://{REDIS_HOST_PORT}")).unwrap();
        let mut conn = client.get_connection().unwrap();
        let _: () = redis::cmd("SELECT")
            .arg(TEST_DATABASE)
            .query(&mut conn)
            .unwrap();
        let _: () = redis::cmd("HSET")
            .arg("imptest:users:1")
            .arg("name")
            .arg("alice")
            .query(&mut conn)
            .unwrap();
        let _: () = redis::cmd("HSET")
            .arg("imptest:users:2")
            .arg("name")
            .arg("bob")
            .query(&mut conn)
            .unwrap();
        let _: () = redis::cmd("SADD")
            .arg("imptest:tags:post1")
            .arg("rust")
            .query(&mut conn)
            .unwrap();

        // Create a target schema for import
        Spi::run("CREATE SCHEMA IF NOT EXISTS import_target;").unwrap();

        // IMPORT FOREIGN SCHEMA should discover tables
        let result = Spi::run(&format!(
            "IMPORT FOREIGN SCHEMA \"public\" FROM SERVER {IMPORT_SERVER_NAME} INTO import_target;"
        ));

        // The import should succeed (may create tables based on discovered keys)
        assert!(
            result.is_ok(),
            "IMPORT FOREIGN SCHEMA should succeed: {:?}",
            result.err()
        );

        // Cleanup
        let _ = Spi::run("DROP SCHEMA IF EXISTS import_target CASCADE;");
        flush_pattern("imptest:*");
        teardown();
    }

    #[pg_test]
    fn test_import_foreign_schema_limit_to() {
        setup();
        setup_import_server();
        flush_pattern("impft:*");

        let client = redis::Client::open(format!("redis://{REDIS_HOST_PORT}")).unwrap();
        let mut conn = client.get_connection().unwrap();
        let _: () = redis::cmd("SELECT")
            .arg(TEST_DATABASE)
            .query(&mut conn)
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg("impft:config:timeout")
            .arg("30")
            .query(&mut conn)
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg("impft:cache:key1")
            .arg("val1")
            .query(&mut conn)
            .unwrap();

        Spi::run("CREATE SCHEMA IF NOT EXISTS import_limit_target;").unwrap();

        // LIMIT TO a specific table name (derived from key prefix)
        let result = Spi::run(&format!(
            "IMPORT FOREIGN SCHEMA \"public\" LIMIT TO (impft_config) \
             FROM SERVER {IMPORT_SERVER_NAME} INTO import_limit_target;"
        ));

        assert!(
            result.is_ok(),
            "IMPORT FOREIGN SCHEMA LIMIT TO should succeed: {:?}",
            result.err()
        );

        let _ = Spi::run("DROP SCHEMA IF EXISTS import_limit_target CASCADE;");
        flush_pattern("impft:*");
        teardown();
    }

    // ================================================
    // BATCH SIZE CONFIGURATION TESTS
    // ================================================

    #[pg_test]
    fn test_batch_size_default() {
        setup();
        let table = "batchdef_hash";
        flush_key("batchdef:hash");
        // No batch_size option → default should be used (1000)
        create_table(table, "key text, value text", "hash", "batchdef:hash");

        Spi::run(&format!(
            "INSERT INTO {table} VALUES ('k1', 'v1'), ('k2', 'v2');"
        ))
        .unwrap();

        let cnt = count(table);
        assert_eq!(cnt, 2, "Default batch should still work for small inserts");

        drop_table(table);
        flush_key("batchdef:hash");
        teardown();
    }

    #[pg_test]
    fn test_batch_insert_with_ttl() {
        setup();
        let table = "batch_ttl_hash";
        flush_key("batch:ttl:hash");
        Spi::run(&format!(
            "CREATE FOREIGN TABLE {table} (key text, value text, ttl bigint) \
             SERVER {SERVER_NAME} OPTIONS (\
                database '{TEST_DATABASE}', table_type 'hash', \
                table_key_prefix 'batch:ttl:hash', batch_size '500');"
        ))
        .unwrap();

        // Insert with explicit TTL
        Spi::run(&format!(
            "INSERT INTO {table} (key, value, ttl) VALUES ('f1', 'v1', 3600), ('f2', 'v2', 7200);"
        ))
        .unwrap();

        let cnt = count(table);
        assert_eq!(cnt, 2, "Batch insert with TTL should produce 2 rows");

        // Check that TTL was applied
        let ttl_val =
            Spi::get_one::<i64>(&format!("SELECT ttl FROM {table} WHERE key = 'f1';")).unwrap();

        assert!(
            ttl_val.is_some() && ttl_val.unwrap() > 0,
            "TTL should be set and positive"
        );

        drop_table(table);
        flush_key("batch:ttl:hash");
        teardown();
    }

    // ================================================
    // EXPLAIN WITH PUSHDOWN TESTS
    // ================================================

    #[pg_test]
    fn test_explain_with_pushdown_condition() {
        setup();
        let table = "explain_pd";
        flush_key("explain:pd:data");
        create_table(table, "key text, value text", "hash", "explain:pd:data");

        Spi::run(&format!(
            "INSERT INTO {table} VALUES ('k1', 'v1'), ('k2', 'v2'), ('k3', 'v3');"
        ))
        .unwrap();

        // EXPLAIN a query with WHERE clause pushdown
        let explain_output = Spi::get_one::<String>(&format!(
            "EXPLAIN (FORMAT TEXT) SELECT * FROM {table} WHERE key = 'k1';"
        ))
        .unwrap()
        .unwrap();

        assert!(
            explain_output.contains("Foreign Scan") || explain_output.contains("Scan"),
            "EXPLAIN with pushdown should show scan info"
        );

        drop_table(table);
        flush_key("explain:pd:data");
        teardown();
    }

    // ================================================
    // ANALYZE TESTS
    // ================================================

    #[pg_test]
    fn test_analyze_foreign_table_hash() {
        setup();
        let table = "analyze_hash";
        flush_key("analyze:hash:data");
        create_table(table, "key text, value text", "hash", "analyze:hash:data");

        Spi::run(&format!(
            "INSERT INTO {table} VALUES ('f1', 'v1'), ('f2', 'v2'), ('f3', 'v3'), ('f4', 'v4'), ('f5', 'v5');"
        ))
        .unwrap();

        let result = Spi::run(&format!("ANALYZE {table};"));
        assert!(result.is_ok(), "ANALYZE should succeed on hash table");

        let reltuples = Spi::get_one::<f32>(&format!(
            "SELECT reltuples FROM pg_class WHERE relname = '{table}';"
        ))
        .unwrap()
        .unwrap_or(0.0);

        assert!(
            reltuples > 0.0,
            "pg_class.reltuples should be updated after ANALYZE, got {}",
            reltuples
        );

        drop_table(table);
        flush_key("analyze:hash:data");
        teardown();
    }

    #[pg_test]
    fn test_analyze_foreign_table_set() {
        setup();
        let table = "analyze_set";
        flush_key("analyze:set:data");
        create_table(table, "member text", "set", "analyze:set:data");

        Spi::run(&format!(
            "INSERT INTO {table} VALUES ('m1'), ('m2'), ('m3');"
        ))
        .unwrap();

        let result = Spi::run(&format!("ANALYZE {table};"));
        assert!(result.is_ok(), "ANALYZE should succeed on set table");

        let reltuples = Spi::get_one::<f32>(&format!(
            "SELECT reltuples FROM pg_class WHERE relname = '{table}';"
        ))
        .unwrap()
        .unwrap_or(0.0);

        assert!(
            reltuples > 0.0,
            "pg_class.reltuples should be updated after ANALYZE for set, got {}",
            reltuples
        );

        drop_table(table);
        flush_key("analyze:set:data");
        teardown();
    }

    #[pg_test]
    fn test_analyze_foreign_table_multi_key() {
        setup();
        let table = "analyze_mk";
        flush_pattern("analyzemk:*");
        create_table(table, "key text, value text", "string", "analyzemk:*");

        Spi::run(&format!(
            "INSERT INTO {table} VALUES ('analyzemk:k1', 'v1'), ('analyzemk:k2', 'v2'), ('analyzemk:k3', 'v3');"
        ))
        .unwrap();

        let result = Spi::run(&format!("ANALYZE {table};"));
        assert!(result.is_ok(), "ANALYZE should succeed on multi-key table");

        let reltuples = Spi::get_one::<f32>(&format!(
            "SELECT reltuples FROM pg_class WHERE relname = '{table}';"
        ))
        .unwrap()
        .unwrap_or(0.0);

        assert!(
            reltuples > 0.0,
            "pg_class.reltuples should be updated after ANALYZE for multi-key, got {}",
            reltuples
        );

        drop_table(table);
        flush_pattern("analyzemk:*");
        teardown();
    }

    // ================================================
    // BEGIN/END FOREIGN INSERT TESTS (COPY FROM, INSERT SELECT)
    // ================================================

    #[pg_test]
    fn test_insert_select_cross_table() {
        setup();
        let src_table = "ins_sel_src";
        let dst_table = "ins_sel_dst";
        flush_key("inssel:src");
        flush_key("inssel:dst");
        create_table(src_table, "key text, value text", "hash", "inssel:src");
        create_table(dst_table, "key text, value text", "hash", "inssel:dst");

        Spi::run(&format!(
            "INSERT INTO {src_table} VALUES ('f1', 'v1'), ('f2', 'v2'), ('f3', 'v3');"
        ))
        .unwrap();

        // INSERT INTO ... SELECT ... triggers BeginForeignInsert
        let result = Spi::run(&format!(
            "INSERT INTO {dst_table} SELECT * FROM {src_table};"
        ));
        assert!(
            result.is_ok(),
            "INSERT INTO ... SELECT should succeed: {:?}",
            result.err()
        );

        let cnt = count(dst_table);
        assert_eq!(
            cnt, 3,
            "Destination table should have 3 rows from INSERT SELECT"
        );

        drop_table(src_table);
        drop_table(dst_table);
        flush_key("inssel:src");
        flush_key("inssel:dst");
        teardown();
    }

    #[pg_test]
    fn test_insert_select_list() {
        setup();
        let src_table = "ins_sel_list_src";
        let dst_table = "ins_sel_list_dst";
        flush_key("inssel:list:src");
        flush_key("inssel:list:dst");
        create_table(src_table, "element text", "list", "inssel:list:src");
        create_table(dst_table, "element text", "list", "inssel:list:dst");

        Spi::run(&format!(
            "INSERT INTO {src_table} VALUES ('a'), ('b'), ('c');"
        ))
        .unwrap();

        let result = Spi::run(&format!(
            "INSERT INTO {dst_table} SELECT * FROM {src_table};"
        ));
        assert!(
            result.is_ok(),
            "INSERT INTO ... SELECT for list should succeed: {:?}",
            result.err()
        );

        let cnt = count(dst_table);
        assert_eq!(cnt, 3, "Destination list should have 3 elements");

        drop_table(src_table);
        drop_table(dst_table);
        flush_key("inssel:list:src");
        flush_key("inssel:list:dst");
        teardown();
    }

    // ================================================
    // RECHECK / RESCAN TESTS
    // ================================================

    #[pg_test]
    fn test_rescan_in_nested_loop() {
        setup();
        let table = "rescan_hash";
        flush_key("rescan:hash");
        create_table(table, "key text, value text", "hash", "rescan:hash");

        Spi::run(&format!(
            "INSERT INTO {table} VALUES ('k1', 'v1'), ('k2', 'v2'), ('k3', 'v3');"
        ))
        .unwrap();

        // Create a local table to join with (forces nested loop / rescan)
        Spi::run(
            "CREATE TEMPORARY TABLE local_keys (k text); \
             INSERT INTO local_keys VALUES ('k1'), ('k2');",
        )
        .unwrap();

        let cnt = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM local_keys lk, {table} ft WHERE lk.k = ft.key;"
        ))
        .unwrap()
        .unwrap();

        assert_eq!(cnt, 2, "Join should return 2 matching rows");

        drop_table(table);
        let _ = Spi::run("DROP TABLE IF EXISTS local_keys;");
        flush_key("rescan:hash");
        teardown();
    }

    // ================================================
    // BATCH INSERT REGRESSION TESTS
    // ================================================

    #[pg_test]
    fn test_batch_insert_regression_after_refactor() {
        setup();
        let table = "batch_regr_hash";
        flush_key("batch:regr:hash");
        create_table_with_batch(
            table,
            "key text, value text",
            "hash",
            "batch:regr:hash",
            100,
        );

        // Insert a larger batch to verify pipeline still works
        let mut values = Vec::new();
        for i in 0..50 {
            values.push(format!("('field_{i}', 'value_{i}')"));
        }
        let values_str = values.join(", ");
        Spi::run(&format!("INSERT INTO {table} VALUES {values_str};")).unwrap();

        let cnt = count(table);
        assert_eq!(
            cnt, 50,
            "Batch insert regression: should have 50 rows after refactor"
        );

        // Verify individual row correctness
        let val = Spi::get_one::<String>(&format!(
            "SELECT value FROM {table} WHERE key = 'field_25';"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(val, "value_25", "Row content should be correct");

        drop_table(table);
        flush_key("batch:regr:hash");
        teardown();
    }

    #[pg_test]
    fn test_import_foreign_schema_stream_not_multi_key() {
        setup();
        setup_import_server();
        flush_pattern("impstream:*");

        let client = redis::Client::open(format!("redis://{REDIS_HOST_PORT}")).unwrap();
        let mut conn = client.get_connection().unwrap();
        let _: () = redis::cmd("SELECT")
            .arg(TEST_DATABASE)
            .query(&mut conn)
            .unwrap();

        // Create two stream keys with shared prefix
        let _: () = redis::cmd("XADD")
            .arg("impstream:events:1")
            .arg("*")
            .arg("action")
            .arg("click")
            .query(&mut conn)
            .unwrap();
        let _: () = redis::cmd("XADD")
            .arg("impstream:events:2")
            .arg("*")
            .arg("action")
            .arg("scroll")
            .query(&mut conn)
            .unwrap();

        Spi::run("CREATE SCHEMA IF NOT EXISTS import_stream_test;").unwrap();

        let result = Spi::run(&format!(
            "IMPORT FOREIGN SCHEMA \"public\" FROM SERVER {IMPORT_SERVER_NAME} INTO import_stream_test;"
        ));
        assert!(
            result.is_ok(),
            "IMPORT FOREIGN SCHEMA with streams should succeed: {:?}",
            result.err()
        );

        // Each stream should get its own table (not grouped by prefix)
        // Verify by querying one of the imported tables
        let tables: Vec<String> = Spi::connect(|client| {
            let mut tables = Vec::new();
            let query = "SELECT foreign_table_name::text FROM information_schema.foreign_tables \
                         WHERE foreign_table_schema = 'import_stream_test' \
                         AND foreign_table_name::text LIKE 'impstream%' \
                         ORDER BY foreign_table_name;";
            let result = client.select(query, None, &[]).unwrap();
            for row in result {
                if let Some(name) = row.get::<&str>(1).unwrap() {
                    tables.push(name.to_string());
                }
            }
            tables
        });

        // Should have 2 separate stream tables (one per key), not 1 grouped table
        assert!(
            tables.len() >= 2,
            "Expected at least 2 stream tables (one per key), got {}: {:?}",
            tables.len(),
            tables
        );

        // Verify the imported stream tables are queryable (not multi-key, so no validation error)
        for table_name in &tables {
            let query = format!(
                "SELECT COUNT(*) FROM import_stream_test.\"{}\";",
                table_name
            );
            let cnt = Spi::get_one::<i64>(&query).unwrap().unwrap();
            assert!(
                cnt >= 1,
                "Stream table {} should have at least 1 entry",
                table_name
            );
        }

        let _ = Spi::run("DROP SCHEMA IF EXISTS import_stream_test CASCADE;");
        flush_pattern("impstream:*");
        teardown();
    }

    #[pg_test]
    fn test_import_foreign_schema_derive_prefix_no_colon() {
        setup();
        setup_import_server();
        flush_pattern("simplekey*");

        let client = redis::Client::open(format!("redis://{REDIS_HOST_PORT}")).unwrap();
        let mut conn = client.get_connection().unwrap();
        let _: () = redis::cmd("SELECT")
            .arg(TEST_DATABASE)
            .query(&mut conn)
            .unwrap();

        // Create a key without colon separator
        let _: () = redis::cmd("SET")
            .arg("simplekey")
            .arg("hello")
            .query(&mut conn)
            .unwrap();

        Spi::run("CREATE SCHEMA IF NOT EXISTS import_simple_test;").unwrap();

        let result = Spi::run(&format!(
            "IMPORT FOREIGN SCHEMA \"public\" FROM SERVER {IMPORT_SERVER_NAME} INTO import_simple_test;"
        ));
        assert!(
            result.is_ok(),
            "IMPORT should succeed for keys without colon: {:?}",
            result.err()
        );

        // The table should be queryable and return the value
        let tables: Vec<String> = Spi::connect(|client| {
            let mut tables = Vec::new();
            let query = "SELECT foreign_table_name::text FROM information_schema.foreign_tables \
                         WHERE foreign_table_schema = 'import_simple_test' \
                         AND foreign_table_name::text LIKE 'simplekey%';";
            let result = client.select(query, None, &[]).unwrap();
            for row in result {
                if let Some(name) = row.get::<&str>(1).unwrap() {
                    tables.push(name.to_string());
                }
            }
            tables
        });

        assert!(
            !tables.is_empty(),
            "Should have imported at least one table for 'simplekey'"
        );

        // Query the imported table — should return data
        let query = format!("SELECT COUNT(*) FROM import_simple_test.\"{}\";", tables[0]);
        let cnt = Spi::get_one::<i64>(&query).unwrap().unwrap();
        assert!(cnt >= 1, "Imported table should have data");

        let _ = Spi::run("DROP SCHEMA IF EXISTS import_simple_test CASCADE;");
        flush_pattern("simplekey*");
        teardown();
    }

    #[pg_test]
    fn test_import_foreign_schema_zset_column_order() {
        setup();
        setup_import_server();
        flush_pattern("impzset:*");

        let client = redis::Client::open(format!("redis://{REDIS_HOST_PORT}")).unwrap();
        let mut conn = client.get_connection().unwrap();
        let _: () = redis::cmd("SELECT")
            .arg(TEST_DATABASE)
            .query(&mut conn)
            .unwrap();

        // Create a zset with known member/score
        let _: () = redis::cmd("ZADD")
            .arg("impzset:scores:1")
            .arg(42.5f64)
            .arg("alice")
            .query(&mut conn)
            .unwrap();

        Spi::run("CREATE SCHEMA IF NOT EXISTS import_zset_test;").unwrap();

        let result = Spi::run(&format!(
            "IMPORT FOREIGN SCHEMA \"public\" FROM SERVER {IMPORT_SERVER_NAME} INTO import_zset_test;"
        ));
        assert!(
            result.is_ok(),
            "IMPORT should succeed for zset: {:?}",
            result.err()
        );

        // Find the imported zset table
        let tables: Vec<String> = Spi::connect(|client| {
            let mut tables = Vec::new();
            let query = "SELECT foreign_table_name::text FROM information_schema.foreign_tables \
                         WHERE foreign_table_schema = 'import_zset_test' \
                         AND foreign_table_name::text LIKE 'impzset%';";
            let result = client.select(query, None, &[]).unwrap();
            for row in result {
                if let Some(name) = row.get::<&str>(1).unwrap() {
                    tables.push(name.to_string());
                }
            }
            tables
        });

        assert!(!tables.is_empty(), "Should have imported zset table");

        // Query and verify column order: key, member, score
        let query = format!(
            "SELECT member, score FROM import_zset_test.\"{}\" LIMIT 1;",
            tables[0]
        );
        let row = Spi::connect(|client| {
            let result = client.select(&query, None, &[]).unwrap();
            let mut rows = Vec::new();
            for r in result {
                let member = r.get::<&str>(1).unwrap().unwrap_or("").to_string();
                let score = r.get::<&str>(2).unwrap().unwrap_or("").to_string();
                rows.push((member, score));
            }
            rows
        });

        assert_eq!(row.len(), 1, "Should have 1 row");
        assert_eq!(row[0].0, "alice", "member column should be 'alice'");
        assert!(
            row[0].1.starts_with("42"),
            "score column should start with '42', got '{}'",
            row[0].1
        );

        let _ = Spi::run("DROP SCHEMA IF EXISTS import_zset_test CASCADE;");
        flush_pattern("impzset:*");
        teardown();
    }
}
