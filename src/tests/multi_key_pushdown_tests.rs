#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    const REDIS_HOST_PORT: &str = "127.0.0.1:8899";
    const TEST_DATABASE: &str = "15";
    const FDW_NAME: &str = "redis_mkpd_fdw";
    const SERVER_NAME: &str = "redis_mkpd_server";

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

    fn redis_conn() -> redis::Connection {
        redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap()
    }

    fn seed_string_keys(prefix: &str, count: usize) {
        let mut conn = redis_conn();
        let mut pipe = redis::pipe();
        for i in 1..=count {
            pipe.cmd("SET")
                .arg(format!("{}:{}", prefix, i))
                .arg(format!("value_{}", i));
        }
        let _: () = pipe.query(&mut conn).unwrap();
    }

    fn cleanup_string_keys(prefix: &str, count: usize) {
        let mut conn = redis_conn();
        let mut pipe = redis::pipe();
        for i in 1..=count {
            pipe.cmd("DEL").arg(format!("{}:{}", prefix, i));
        }
        let _: Vec<i32> = pipe.query(&mut conn).unwrap();
    }

    #[pg_test]
    fn test_multi_key_string_pushdown_equal() {
        setup_fdw();
        let prefix = "mkpd_str_eq";
        cleanup_string_keys(prefix, 20);
        seed_string_keys(prefix, 20);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mkpd_str_eq_tbl (key text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}:*'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        let result = Spi::get_one::<String>(&format!(
            "SELECT value FROM mkpd_str_eq_tbl WHERE key = '{}:5';",
            prefix
        ))
        .unwrap();
        assert_eq!(result, Some("value_5".to_string()));

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM mkpd_str_eq_tbl WHERE key = '{}:5';",
            prefix
        ))
        .unwrap();
        assert_eq!(count, Some(1));

        Spi::run("DROP FOREIGN TABLE mkpd_str_eq_tbl;").unwrap();
        cleanup_string_keys(prefix, 20);
        cleanup();
    }

    #[pg_test]
    fn test_multi_key_string_pushdown_like() {
        setup_fdw();
        let prefix = "mkpd_str_like";
        cleanup_string_keys(prefix, 20);
        seed_string_keys(prefix, 20);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mkpd_str_like_tbl (key text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}:*'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        // Keys 1, 10-19 match LIKE 'mkpd_str_like:1%' → 11 keys
        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM mkpd_str_like_tbl WHERE key LIKE '{}:1%';",
            prefix
        ))
        .unwrap();
        assert_eq!(count, Some(11), "Expected keys 1,10-19 = 11 matches");

        Spi::run("DROP FOREIGN TABLE mkpd_str_like_tbl;").unwrap();
        cleanup_string_keys(prefix, 20);
        cleanup();
    }

    #[pg_test]
    fn test_multi_key_string_pushdown_in() {
        setup_fdw();
        let prefix = "mkpd_str_in";
        cleanup_string_keys(prefix, 10);
        seed_string_keys(prefix, 10);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mkpd_str_in_tbl (key text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}:*'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM mkpd_str_in_tbl WHERE key IN ('{}:2', '{}:5', '{}:8');",
            prefix, prefix, prefix
        ))
        .unwrap();
        assert_eq!(count, Some(3));

        Spi::run("DROP FOREIGN TABLE mkpd_str_in_tbl;").unwrap();
        cleanup_string_keys(prefix, 10);
        cleanup();
    }

    #[pg_test]
    fn test_multi_key_string_pushdown_equal_not_found() {
        setup_fdw();
        let prefix = "mkpd_str_nf";
        cleanup_string_keys(prefix, 5);
        seed_string_keys(prefix, 5);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mkpd_str_nf_tbl (key text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}:*'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM mkpd_str_nf_tbl WHERE key = '{}:99';",
            prefix
        ))
        .unwrap();
        assert_eq!(count, Some(0));

        Spi::run("DROP FOREIGN TABLE mkpd_str_nf_tbl;").unwrap();
        cleanup_string_keys(prefix, 5);
        cleanup();
    }

    #[pg_test]
    fn test_multi_key_hash_pushdown_equal() {
        setup_fdw();
        let prefix = "mkpd_hash_eq";

        let mut conn = redis_conn();
        for i in 1..=5 {
            let key = format!("{}:{}", prefix, i);
            let _: () = redis::cmd("DEL").arg(&key).query(&mut conn).unwrap();
            let _: () = redis::cmd("HSET")
                .arg(&key)
                .arg("name")
                .arg(format!("user_{}", i))
                .query(&mut conn)
                .unwrap();
        }

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mkpd_hash_eq_tbl (key text, field text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix '{}:*'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        let result = Spi::get_one::<String>(&format!(
            "SELECT value FROM mkpd_hash_eq_tbl WHERE key = '{}:3';",
            prefix
        ))
        .unwrap();
        assert_eq!(result, Some("user_3".to_string()));

        Spi::run("DROP FOREIGN TABLE mkpd_hash_eq_tbl;").unwrap();
        for i in 1..=5 {
            let _: () = redis::cmd("DEL")
                .arg(format!("{}:{}", prefix, i))
                .query(&mut conn)
                .unwrap();
        }
        cleanup();
    }

    #[pg_test]
    fn test_multi_key_pushdown_with_ttl_column() {
        setup_fdw();
        let prefix = "mkpd_str_ttl";
        cleanup_string_keys(prefix, 5);
        seed_string_keys(prefix, 5);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mkpd_str_ttl_tbl (key text, value text, ttl bigint) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}:*'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM mkpd_str_ttl_tbl WHERE key = '{}:3';",
            prefix
        ))
        .unwrap();
        assert_eq!(count, Some(1));

        Spi::run("DROP FOREIGN TABLE mkpd_str_ttl_tbl;").unwrap();
        cleanup_string_keys(prefix, 5);
        cleanup();
    }

    #[pg_test]
    fn test_multi_key_pushdown_with_ttl_first() {
        setup_fdw();
        let prefix = "mkpd_str_ttl0";
        cleanup_string_keys(prefix, 5);
        seed_string_keys(prefix, 5);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mkpd_str_ttl0_tbl (ttl bigint, key text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}:*'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM mkpd_str_ttl0_tbl WHERE key = '{}:2';",
            prefix
        ))
        .unwrap();
        assert_eq!(count, Some(1));

        Spi::run("DROP FOREIGN TABLE mkpd_str_ttl0_tbl;").unwrap();
        cleanup_string_keys(prefix, 5);
        cleanup();
    }

    #[pg_test]
    fn test_multi_key_set_pushdown_equal() {
        setup_fdw();
        let prefix = "mkpd_set_eq";

        let mut conn = redis_conn();
        for i in 1..=5 {
            let key = format!("{}:{}", prefix, i);
            let _: () = redis::cmd("DEL").arg(&key).query(&mut conn).unwrap();
            let _: () = redis::cmd("SADD")
                .arg(&key)
                .arg(format!("member_{}", i))
                .query(&mut conn)
                .unwrap();
        }

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mkpd_set_eq_tbl (key text, member text) SERVER {} OPTIONS (
                database '{}', table_type 'set', table_key_prefix '{}:*'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        let result = Spi::get_one::<String>(&format!(
            "SELECT member FROM mkpd_set_eq_tbl WHERE key = '{}:4';",
            prefix
        ))
        .unwrap();
        assert_eq!(result, Some("member_4".to_string()));

        Spi::run("DROP FOREIGN TABLE mkpd_set_eq_tbl;").unwrap();
        for i in 1..=5 {
            let _: () = redis::cmd("DEL")
                .arg(format!("{}:{}", prefix, i))
                .query(&mut conn)
                .unwrap();
        }
        cleanup();
    }

    #[pg_test]
    fn test_multi_key_zset_pushdown_like() {
        setup_fdw();
        let prefix = "mkpd_zset_lk";

        let mut conn = redis_conn();
        for i in 1..=15 {
            let key = format!("{}:{}", prefix, i);
            let _: () = redis::cmd("DEL").arg(&key).query(&mut conn).unwrap();
            let _: () = redis::cmd("ZADD")
                .arg(&key)
                .arg(i as f64)
                .arg(format!("item_{}", i))
                .query(&mut conn)
                .unwrap();
        }

        Spi::run(&format!(
            "CREATE FOREIGN TABLE mkpd_zset_lk_tbl (key text, member text, score float8) SERVER {} OPTIONS (
                database '{}', table_type 'zset', table_key_prefix '{}:*'
            );",
            SERVER_NAME, TEST_DATABASE, prefix
        ))
        .unwrap();

        // LIKE 'mkpd_zset_lk:1%' should match keys 1, 10-15 = 7 keys
        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM mkpd_zset_lk_tbl WHERE key LIKE '{}:1%';",
            prefix
        ))
        .unwrap();
        assert_eq!(count, Some(7), "Expected keys 1,10-15 = 7 matches");

        Spi::run("DROP FOREIGN TABLE mkpd_zset_lk_tbl;").unwrap();
        for i in 1..=15 {
            let _: () = redis::cmd("DEL")
                .arg(format!("{}:{}", prefix, i))
                .query(&mut conn)
                .unwrap();
        }
        cleanup();
    }
}
