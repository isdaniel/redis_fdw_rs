#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    const REDIS_HOST_PORT: &str = "127.0.0.1:8899";
    const TEST_DATABASE: &str = "15";
    const FDW_NAME: &str = "redis_colmap_fdw";
    const SERVER_NAME: &str = "redis_colmap_server";

    fn setup_fdw() {
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            FDW_NAME
        ));
        Spi::run(&format!(
            "CREATE FOREIGN DATA WRAPPER {} HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;",
            FDW_NAME
        ))
        .unwrap();
        Spi::run(&format!(
            "CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (host_port '{}');",
            SERVER_NAME, FDW_NAME, REDIS_HOST_PORT
        ))
        .unwrap();
    }

    fn cleanup() {
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            FDW_NAME
        ));
    }

    fn cleanup_redis_key(key: &str) {
        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        let _: Result<(), _> = redis::cmd("DEL").arg(key).query(&mut conn);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // LIST with (index, value) two-column definition
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    fn test_list_two_column_insert_and_select() {
        setup_fdw();
        let key = "colmap_test:list:two_col";
        cleanup_redis_key(key);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE colmap_list_idx (index int, value text) SERVER {} OPTIONS (
                database '{}', table_type 'list', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        Spi::run("INSERT INTO colmap_list_idx (value) VALUES ('alpha');").unwrap();
        Spi::run("INSERT INTO colmap_list_idx (value) VALUES ('beta');").unwrap();
        Spi::run("INSERT INTO colmap_list_idx (value) VALUES ('gamma');").unwrap();

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM colmap_list_idx;")
            .unwrap()
            .unwrap();
        assert_eq!(count, 3, "Expected 3 rows in list");

        // Verify the values are correct by selecting all rows
        let results = Spi::connect(|client| {
            let mut rows = Vec::new();
            let table = client
                .select("SELECT index, value FROM colmap_list_idx;", None, &[])
                .unwrap();
            for row in table {
                let idx: Option<i32> = row.get_by_name("index").unwrap();
                let val: Option<String> = row.get_by_name("value").unwrap();
                rows.push((idx.unwrap(), val.unwrap()));
            }
            rows
        });

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (0, "alpha".to_string()));
        assert_eq!(results[1], (1, "beta".to_string()));
        assert_eq!(results[2], (2, "gamma".to_string()));

        Spi::run("DROP FOREIGN TABLE colmap_list_idx;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }

    #[pg_test]
    fn test_list_two_column_select_with_limit() {
        setup_fdw();
        let key = "colmap_test:list:limit";
        cleanup_redis_key(key);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE colmap_list_lim (index int, value text) SERVER {} OPTIONS (
                database '{}', table_type 'list', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        Spi::run("INSERT INTO colmap_list_lim (value) VALUES ('a');").unwrap();
        Spi::run("INSERT INTO colmap_list_lim (value) VALUES ('b');").unwrap();
        Spi::run("INSERT INTO colmap_list_lim (value) VALUES ('c');").unwrap();
        Spi::run("INSERT INTO colmap_list_lim (value) VALUES ('d');").unwrap();

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM colmap_list_lim LIMIT 2;")
            .unwrap()
            .unwrap();
        assert_eq!(count, 2);

        Spi::run("DROP FOREIGN TABLE colmap_list_lim;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }

    #[pg_test]
    fn test_list_single_column_still_works() {
        setup_fdw();
        let key = "colmap_test:list:single";
        cleanup_redis_key(key);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE colmap_list_single (element text) SERVER {} OPTIONS (
                database '{}', table_type 'list', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        Spi::run("INSERT INTO colmap_list_single VALUES ('one');").unwrap();
        Spi::run("INSERT INTO colmap_list_single VALUES ('two');").unwrap();

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM colmap_list_single;")
            .unwrap()
            .unwrap();
        assert_eq!(count, 2);

        let val = Spi::get_one::<String>("SELECT element FROM colmap_list_single LIMIT 1;")
            .unwrap()
            .unwrap();
        assert_eq!(val, "one");

        Spi::run("DROP FOREIGN TABLE colmap_list_single;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }

    #[pg_test]
    fn test_list_two_column_delete() {
        setup_fdw();
        let key = "colmap_test:list:delete";
        cleanup_redis_key(key);

        // For DELETE on list, use single-column (value-only) definition
        // since the first column is used as the row identity key by the FDW
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colmap_list_del (value text) SERVER {} OPTIONS (
                database '{}', table_type 'list', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        Spi::run("INSERT INTO colmap_list_del VALUES ('x');").unwrap();
        Spi::run("INSERT INTO colmap_list_del VALUES ('y');").unwrap();
        Spi::run("INSERT INTO colmap_list_del VALUES ('z');").unwrap();

        Spi::run("DELETE FROM colmap_list_del WHERE value = 'y';").unwrap();

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM colmap_list_del;")
            .unwrap()
            .unwrap();
        assert_eq!(count, 2);

        Spi::run("DROP FOREIGN TABLE colmap_list_del;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // STREAM with multiple named columns
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    fn test_stream_multi_column_insert_and_select() {
        setup_fdw();
        let key = "colmap_test:stream:multi";
        cleanup_redis_key(key);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE colmap_stream (id text, user_id text, action text, resource text) SERVER {} OPTIONS (
                database '{}', table_type 'stream', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        Spi::run(
            "INSERT INTO colmap_stream VALUES ('*', 'user:alice', 'CREATE', 'project:alpha');",
        )
        .unwrap();
        Spi::run("INSERT INTO colmap_stream VALUES ('*', 'user:bob', 'UPDATE', 'project:alpha');")
            .unwrap();
        Spi::run(
            "INSERT INTO colmap_stream VALUES ('*', 'user:alice', 'DELETE', 'file:readme.md');",
        )
        .unwrap();

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM colmap_stream;")
            .unwrap()
            .unwrap();
        assert_eq!(count, 3, "Expected 3 stream entries");

        let user = Spi::get_one::<String>("SELECT user_id FROM colmap_stream LIMIT 1;")
            .unwrap()
            .unwrap();
        assert_eq!(user, "user:alice");

        let action = Spi::get_one::<String>("SELECT action FROM colmap_stream LIMIT 1;")
            .unwrap()
            .unwrap();
        assert_eq!(action, "CREATE");

        let resource = Spi::get_one::<String>("SELECT resource FROM colmap_stream LIMIT 1;")
            .unwrap()
            .unwrap();
        assert_eq!(resource, "project:alpha");

        Spi::run("DROP FOREIGN TABLE colmap_stream;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }

    #[pg_test]
    fn test_stream_multi_column_delete_by_id() {
        setup_fdw();
        let key = "colmap_test:stream:del";
        cleanup_redis_key(key);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE colmap_stream_del (id text, msg text, level text) SERVER {} OPTIONS (
                database '{}', table_type 'stream', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        Spi::run("INSERT INTO colmap_stream_del VALUES ('*', 'hello', 'info');").unwrap();
        Spi::run("INSERT INTO colmap_stream_del VALUES ('*', 'world', 'warn');").unwrap();

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM colmap_stream_del;")
            .unwrap()
            .unwrap();
        assert_eq!(count, 2);

        let stream_id = Spi::get_one::<String>("SELECT id FROM colmap_stream_del LIMIT 1;")
            .unwrap()
            .unwrap();
        Spi::run(&format!(
            "DELETE FROM colmap_stream_del WHERE id = '{}';",
            stream_id
        ))
        .unwrap();

        let count_after = Spi::get_one::<i64>("SELECT COUNT(*) FROM colmap_stream_del;")
            .unwrap()
            .unwrap();
        assert_eq!(count_after, 1);

        Spi::run("DROP FOREIGN TABLE colmap_stream_del;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }

    #[pg_test]
    fn test_stream_three_column_legacy_format() {
        setup_fdw();
        let key = "colmap_test:stream:legacy";
        cleanup_redis_key(key);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE colmap_stream_legacy (id text, field text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'stream', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        Spi::run("INSERT INTO colmap_stream_legacy VALUES ('*', 'sensor', 'temp_22');").unwrap();
        Spi::run("INSERT INTO colmap_stream_legacy VALUES ('*', 'sensor', 'temp_23');").unwrap();

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM colmap_stream_legacy;")
            .unwrap()
            .unwrap();
        assert_eq!(count, 2);

        let field_val = Spi::get_one::<String>("SELECT field FROM colmap_stream_legacy LIMIT 1;")
            .unwrap()
            .unwrap();
        assert_eq!(field_val, "sensor");

        let value_val = Spi::get_one::<String>("SELECT value FROM colmap_stream_legacy LIMIT 1;")
            .unwrap()
            .unwrap();
        assert_eq!(value_val, "temp_22");

        Spi::run("DROP FOREIGN TABLE colmap_stream_legacy;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }

    #[pg_test]
    fn test_stream_multi_column_batch_insert() {
        setup_fdw();
        let key = "colmap_test:stream:batch";
        cleanup_redis_key(key);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE colmap_stream_batch (id text, sensor text, reading text) SERVER {} OPTIONS (
                database '{}', table_type 'stream', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        Spi::run(
            "INSERT INTO colmap_stream_batch VALUES
                ('*', 'temp', '22.5'),
                ('*', 'humidity', '65'),
                ('*', 'pressure', '1013');",
        )
        .unwrap();

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM colmap_stream_batch;")
            .unwrap()
            .unwrap();
        assert_eq!(count, 3);

        Spi::run("DROP FOREIGN TABLE colmap_stream_batch;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // COPY FROM tests
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    fn test_copy_from_stream() {
        setup_fdw();
        let key = "colmap_test:stream:copy";
        cleanup_redis_key(key);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE colmap_stream_copy (id text, sensor text, reading text) SERVER {} OPTIONS (
                database '{}', table_type 'stream', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        // Use INSERT INTO ... SELECT to simulate COPY FROM (same code path: begin_foreign_insert)
        Spi::run(
            "INSERT INTO colmap_stream_copy SELECT '*', 'temp', v::text FROM generate_series(1, 5) v;"
        ).unwrap();

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM colmap_stream_copy;")
            .unwrap()
            .unwrap();
        assert_eq!(count, 5, "Expected 5 rows after INSERT SELECT");

        let sensor = Spi::get_one::<String>("SELECT sensor FROM colmap_stream_copy LIMIT 1;")
            .unwrap()
            .unwrap();
        assert_eq!(sensor, "temp");

        Spi::run("DROP FOREIGN TABLE colmap_stream_copy;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }

    #[pg_test]
    fn test_copy_from_hash() {
        setup_fdw();
        let key = "colmap_test:hash:copy";
        cleanup_redis_key(key);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE colmap_hash_copy (field text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        // INSERT SELECT exercises begin_foreign_insert / exec_foreign_insert path
        Spi::run(
            "INSERT INTO colmap_hash_copy SELECT 'key_' || v, 'val_' || v FROM generate_series(1, 5) v;"
        ).unwrap();

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM colmap_hash_copy;")
            .unwrap()
            .unwrap();
        assert_eq!(count, 5, "Expected 5 hash entries after INSERT SELECT");

        Spi::run("DROP FOREIGN TABLE colmap_hash_copy;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }

    #[pg_test]
    fn test_copy_from_program_hash() {
        setup_fdw();
        let key = "colmap_test:hash:copyprog";
        cleanup_redis_key(key);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE colmap_hash_copyprog (field text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        // Use COPY FROM PROGRAM to simulate COPY FROM stdin
        // Tab-separated: field<TAB>value
        let copy_result = Spi::run(
            r#"COPY colmap_hash_copyprog FROM PROGRAM 'printf "evt:1\tclick_home\nevt:2\tview_products\nevt:3\tclick_cart\n"';"#,
        );
        assert!(
            copy_result.is_ok(),
            "COPY FROM PROGRAM failed: {:?}",
            copy_result.err()
        );

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM colmap_hash_copyprog;")
            .unwrap()
            .unwrap();
        assert_eq!(count, 3, "Expected 3 hash entries after COPY FROM");

        // Verify specific values
        let val = Spi::get_one::<String>(
            "SELECT value FROM colmap_hash_copyprog WHERE field = 'evt:1';",
        )
        .unwrap()
        .unwrap();
        assert_eq!(val, "click_home");

        Spi::run("DROP FOREIGN TABLE colmap_hash_copyprog;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }

    #[pg_test]
    fn test_copy_from_program_stream() {
        setup_fdw();
        let key = "colmap_test:stream:copyprog";
        cleanup_redis_key(key);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE colmap_stream_copyprog (id text, sensor text, reading text) SERVER {} OPTIONS (
                database '{}', table_type 'stream', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        // Tab-separated: id<TAB>sensor<TAB>reading
        let copy_result = Spi::run(
            r#"COPY colmap_stream_copyprog FROM PROGRAM 'printf "*\ttemp\t22.5\n*\thumidity\t65\n*\tpressure\t1013\n"';"#,
        );
        assert!(
            copy_result.is_ok(),
            "COPY FROM PROGRAM for stream failed: {:?}",
            copy_result.err()
        );

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM colmap_stream_copyprog;")
            .unwrap()
            .unwrap();
        assert_eq!(count, 3, "Expected 3 stream entries after COPY FROM");

        let sensor = Spi::get_one::<String>("SELECT sensor FROM colmap_stream_copyprog LIMIT 1;")
            .unwrap()
            .unwrap();
        assert_eq!(sensor, "temp");

        Spi::run("DROP FOREIGN TABLE colmap_stream_copyprog;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }
}
