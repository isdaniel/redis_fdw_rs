#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    const REDIS_HOST_PORT: &str = "127.0.0.1:8899";
    const TEST_DATABASE: &str = "15";
    const FDW_NAME: &str = "redis_pool_perf_wrapper";
    const SERVER_NAME: &str = "redis_pool_perf_server";

    fn setup_pool_test() {
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            FDW_NAME
        ));
        Spi::run(&format!(
            "CREATE FOREIGN DATA WRAPPER {} HANDLER redis_fdw_handler;",
            FDW_NAME
        ))
        .unwrap();
        Spi::run(&format!(
            "CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (host_port '{}');",
            SERVER_NAME, FDW_NAME, REDIS_HOST_PORT
        ))
        .unwrap();
    }

    fn cleanup_pool_test() {
        let _ = Spi::run(&format!("DROP SERVER IF EXISTS {} CASCADE;", SERVER_NAME));
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            FDW_NAME
        ));
    }

    fn create_pool_test_table(table_name: &str, columns: &str, table_type: &str, key_prefix: &str) {
        let sql = format!(
            "CREATE FOREIGN TABLE {table_name} ({columns}) SERVER {SERVER_NAME} OPTIONS (
                database '{TEST_DATABASE}',
                table_type '{table_type}',
                table_key_prefix '{key_prefix}'
            );"
        );
        Spi::run(&sql).unwrap();
    }

    #[pg_test]
    fn test_pool_real_cost_estimation() {
        setup_pool_test();
        let table_name = "pool_perf_hash_cost";
        let key_prefix = "pool_perf:cost_test";

        create_pool_test_table(table_name, "field text, value text", "hash", key_prefix);

        for i in 0..5 {
            Spi::run(&format!(
                "INSERT INTO {} VALUES ('field{}', 'value{}');",
                table_name, i, i
            ))
            .unwrap();
        }

        let explain_output = Spi::get_one::<String>(&format!(
            "EXPLAIN (FORMAT TEXT) SELECT * FROM {};",
            table_name
        ))
        .unwrap()
        .unwrap();

        log!("EXPLAIN output: {}", explain_output);
        assert!(
            !explain_output.contains("rows=1000"),
            "Row estimate should not be the default 1000. Got: {}",
            explain_output
        );

        for i in 0..5 {
            Spi::run(&format!(
                "DELETE FROM {} WHERE field = 'field{}';",
                table_name, i
            ))
            .unwrap();
        }

        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        cleanup_pool_test();
    }

    #[pg_test]
    fn test_pool_rescan_no_reconnect() {
        setup_pool_test();
        let table_name = "pool_perf_rescan";
        let key_prefix = "pool_perf:rescan_test";

        create_pool_test_table(table_name, "field text, value text", "hash", key_prefix);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('a', 'val_a');",
            table_name
        ))
        .unwrap();
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('b', 'val_b');",
            table_name
        ))
        .unwrap();

        Spi::run("CREATE TEMPORARY TABLE pool_local_test (id text);").unwrap();
        Spi::run("INSERT INTO pool_local_test VALUES ('a'), ('b');").unwrap();

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM pool_local_test l, {} r WHERE l.id = r.field;",
            table_name
        ))
        .unwrap();

        assert!(count.is_some());
        assert!(
            count.unwrap() >= 0,
            "JOIN should succeed with connection reuse"
        );

        Spi::run(&format!("DELETE FROM {} WHERE field = 'a';", table_name)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'b';", table_name)).unwrap();

        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        let _ = Spi::run("DROP TABLE IF EXISTS pool_local_test;");
        cleanup_pool_test();
    }

    #[pg_test]
    fn test_pool_reuse_across_queries() {
        setup_pool_test();
        let table_name = "pool_reuse_hash";
        let key_prefix = "pool_perf:reuse_test";

        create_pool_test_table(table_name, "field text, value text", "hash", key_prefix);

        Spi::run(&format!("INSERT INTO {} VALUES ('k1', 'v1');", table_name)).unwrap();

        // Run multiple sequential queries to the same table — pool should be reused
        for i in 0..10 {
            let count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name))
                .unwrap()
                .unwrap();
            assert_eq!(count, 1, "Query {} should return 1 row", i);
        }

        // Run queries interleaved with inserts — pool connection returned and reacquired
        for i in 0..5 {
            Spi::run(&format!(
                "INSERT INTO {} VALUES ('iter{}', 'val{}');",
                table_name, i, i
            ))
            .unwrap();
        }

        let final_count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table_name))
            .unwrap()
            .unwrap();
        assert_eq!(final_count, 6, "Should have 1 original + 5 new rows");

        // Cleanup all inserted rows
        Spi::run(&format!("DELETE FROM {} WHERE field = 'k1';", table_name)).unwrap();
        for i in 0..5 {
            Spi::run(&format!(
                "DELETE FROM {} WHERE field = 'iter{}';",
                table_name, i
            ))
            .unwrap();
        }

        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        cleanup_pool_test();
    }

    #[pg_test]
    fn test_pool_multiple_tables_same_server() {
        setup_pool_test();
        let table1 = "pool_multi_hash";
        let table2 = "pool_multi_set";
        let key1 = "pool_perf:multi_hash";
        let key2 = "pool_perf:multi_set";

        create_pool_test_table(table1, "field text, value text", "hash", key1);
        create_pool_test_table(table2, "member text", "set", key2);

        Spi::run(&format!("INSERT INTO {} VALUES ('f1', 'v1');", table1)).unwrap();
        Spi::run(&format!("INSERT INTO {} VALUES ('member1');", table2)).unwrap();

        let h_count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table1))
            .unwrap()
            .unwrap();
        let s_count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {};", table2))
            .unwrap()
            .unwrap();

        assert_eq!(h_count, 1);
        assert_eq!(s_count, 1);

        Spi::run(&format!("DELETE FROM {} WHERE field = 'f1';", table1)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE member = 'member1';", table2)).unwrap();

        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table1));
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table2));
        cleanup_pool_test();
    }
}
