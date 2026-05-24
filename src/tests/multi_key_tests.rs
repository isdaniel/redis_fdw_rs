#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    const REDIS_HOST_PORT: &str = "127.0.0.1:8899";
    const TEST_DATABASE: &str = "15";
    const FDW_NAME: &str = "redis_multikey_fdw";
    const SERVER_NAME: &str = "redis_multikey_server";

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

    fn seed_redis_keys(prefix: &str, count: usize) {
        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        for i in 1..=count {
            let key = format!("{}:{}", prefix, i);
            let _: () = redis::cmd("SET")
                .arg(&key)
                .arg(format!("value_{}", i))
                .query(&mut conn)
                .unwrap();
        }
    }

    fn cleanup_redis_keys(prefix: &str, count: usize) {
        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        for i in 1..=count {
            let key = format!("{}:{}", prefix, i);
            let _: Result<(), _> = redis::cmd("DEL").arg(&key).query(&mut conn);
        }
    }

    #[pg_test]
    fn test_multi_key_string_select() {
        setup_fdw();
        let prefix = "mk_test_str";
        cleanup_redis_keys(prefix, 5);
        seed_redis_keys(prefix, 5);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mk_str_select (key text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}:*'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM mk_str_select;").unwrap();
        assert_eq!(count, Some(5), "Expected 5 rows, got {:?}", count);

        Spi::run("DROP FOREIGN TABLE mk_str_select;").unwrap();
        cleanup_redis_keys(prefix, 5);
        cleanup();
    }

    #[pg_test]
    fn test_multi_key_string_insert() {
        setup_fdw();
        let prefix = "mk_test_ins";
        cleanup_redis_keys(prefix, 3);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mk_str_insert (key text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}:*'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        Spi::run(&format!(
            "INSERT INTO mk_str_insert VALUES ('{}:1', 'inserted_1');",
            prefix
        ))
        .unwrap();
        Spi::run(&format!(
            "INSERT INTO mk_str_insert VALUES ('{}:2', 'inserted_2');",
            prefix
        ))
        .unwrap();

        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        let val: Option<String> = redis::cmd("GET")
            .arg(format!("{}:1", prefix))
            .query(&mut conn)
            .unwrap();
        assert_eq!(val, Some("inserted_1".to_string()));

        let val2: Option<String> = redis::cmd("GET")
            .arg(format!("{}:2", prefix))
            .query(&mut conn)
            .unwrap();
        assert_eq!(val2, Some("inserted_2".to_string()));

        Spi::run("DROP FOREIGN TABLE mk_str_insert;").unwrap();
        cleanup_redis_keys(prefix, 3);
        cleanup();
    }

    #[pg_test]
    fn test_multi_key_string_delete() {
        setup_fdw();
        let prefix = "mk_test_del";
        cleanup_redis_keys(prefix, 3);
        seed_redis_keys(prefix, 3);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mk_str_delete (key text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}:*'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        Spi::run(&format!(
            "DELETE FROM mk_str_delete WHERE key = '{}:2';",
            prefix
        ))
        .unwrap();

        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        let exists: bool = redis::cmd("EXISTS")
            .arg(format!("{}:2", prefix))
            .query(&mut conn)
            .unwrap();
        assert!(!exists, "Key should have been deleted");

        let exists1: bool = redis::cmd("EXISTS")
            .arg(format!("{}:1", prefix))
            .query(&mut conn)
            .unwrap();
        assert!(exists1, "Key 1 should still exist");

        Spi::run("DROP FOREIGN TABLE mk_str_delete;").unwrap();
        cleanup_redis_keys(prefix, 3);
        cleanup();
    }

    #[pg_test]
    fn test_multi_key_hash_select() {
        setup_fdw();
        let prefix = "mk_test_hash";

        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        for i in 1..=3 {
            let key = format!("{}:{}", prefix, i);
            let _: Result<(), _> = redis::cmd("DEL").arg(&key).query(&mut conn);
            let _: () = redis::cmd("HSET")
                .arg(&key)
                .arg("field_a")
                .arg(format!("val_{}", i))
                .query(&mut conn)
                .unwrap();
        }

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mk_hash_select (key text, field text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix '{}:*'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM mk_hash_select;").unwrap();
        assert_eq!(count, Some(3), "Expected 3 rows, got {:?}", count);

        Spi::run("DROP FOREIGN TABLE mk_hash_select;").unwrap();
        for i in 1..=3 {
            let _: Result<(), _> = redis::cmd("DEL")
                .arg(format!("{}:{}", prefix, i))
                .query(&mut conn);
        }
        cleanup();
    }

    #[pg_test]
    fn test_multi_key_with_ttl_on_insert() {
        setup_fdw();
        let prefix = "mk_test_ttl";
        cleanup_redis_keys(prefix, 3);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mk_str_ttl (key text, value text, ttl bigint) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}:*'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        Spi::run(&format!(
            "INSERT INTO mk_str_ttl VALUES ('{}:1', 'ttl_val', 45);",
            prefix
        ))
        .unwrap();

        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        let ttl: i64 = redis::cmd("TTL")
            .arg(format!("{}:1", prefix))
            .query(&mut conn)
            .unwrap();
        assert!(ttl > 0 && ttl <= 45, "Expected TTL 1-45, got {}", ttl);

        Spi::run("DROP FOREIGN TABLE mk_str_ttl;").unwrap();
        cleanup_redis_keys(prefix, 3);
        cleanup();
    }

    #[pg_test]
    fn test_is_multi_key_pattern_detection() {
        use crate::core::state_manager::is_multi_key_pattern;

        assert!(is_multi_key_pattern("prefix:*"));
        assert!(is_multi_key_pattern("user:?:name"));
        assert!(is_multi_key_pattern("key:[abc]"));
        assert!(!is_multi_key_pattern("simple:prefix:"));
        assert!(!is_multi_key_pattern("no_glob_here"));
    }

    #[pg_test]
    fn test_multi_key_prefix_mismatch_warning() {
        setup_fdw();
        let prefix = "mk_prefix_warn";

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mk_prefix_warn_tbl (key text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}:*'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        // Insert with matching prefix — should work without issue
        Spi::run(&format!(
            "INSERT INTO mk_prefix_warn_tbl VALUES ('{}:1', 'correct');",
            prefix
        ))
        .unwrap();

        // Insert with mismatching prefix — should still succeed (warning only)
        Spi::run("INSERT INTO mk_prefix_warn_tbl VALUES ('wrong:1', 'mismatch');").unwrap();

        // Only the matching key shows up in SELECT (SCAN pattern won't find 'wrong:1')
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM mk_prefix_warn_tbl;")
            .unwrap()
            .unwrap();
        assert_eq!(
            count, 1,
            "Only the matching-prefix key should appear in SELECT"
        );

        // Cleanup
        Spi::run(&format!(
            "DELETE FROM mk_prefix_warn_tbl WHERE key = '{}:1';",
            prefix
        ))
        .unwrap();
        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        let _: Result<(), _> = redis::cmd("DEL").arg("wrong:1").query(&mut conn);
        Spi::run("DROP FOREIGN TABLE mk_prefix_warn_tbl;").unwrap();
        cleanup();
    }

    #[pg_test]
    #[should_panic(expected = "does not match table pattern")]
    fn test_multi_key_strict_prefix_error() {
        setup_fdw();
        let prefix = "mk_strict";

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mk_strict_tbl (key text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}:*', strict_key_prefix 'true'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        // This should error because strict_key_prefix is true
        Spi::run("INSERT INTO mk_strict_tbl VALUES ('wrong:1', 'should_fail');").unwrap();
    }

    #[pg_test]
    fn test_multi_key_hash_prefix_mismatch_warning() {
        setup_fdw();
        let prefix = "mk_hash_warn";

        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mk_hash_warn_tbl (key text, field text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix '{}:*'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        // Insert with correct prefix
        Spi::run(&format!(
            "INSERT INTO mk_hash_warn_tbl VALUES ('{}:1', 'name', 'Alice');",
            prefix
        ))
        .unwrap();

        // Insert with wrong prefix — should succeed with warning
        Spi::run("INSERT INTO mk_hash_warn_tbl VALUES ('other:1', 'name', 'Bob');").unwrap();

        // Only the matching key shows up in SELECT
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM mk_hash_warn_tbl;")
            .unwrap()
            .unwrap();
        assert_eq!(count, 1, "Only matching-prefix key visible in SELECT");

        // Cleanup
        let _: Result<(), _> = redis::cmd("DEL")
            .arg(format!("{}:1", prefix))
            .query(&mut conn);
        let _: Result<(), _> = redis::cmd("DEL").arg("other:1").query(&mut conn);
        Spi::run("DROP FOREIGN TABLE mk_hash_warn_tbl;").unwrap();
        cleanup();
    }
}
