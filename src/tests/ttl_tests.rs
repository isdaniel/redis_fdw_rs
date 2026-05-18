#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    const REDIS_HOST_PORT: &str = "127.0.0.1:8899";
    const TEST_DATABASE: &str = "15";
    const FDW_NAME: &str = "redis_ttl_fdw";
    const SERVER_NAME: &str = "redis_ttl_server";

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

    #[pg_test]
    fn test_ttl_default_via_table_option_string() {
        setup_fdw();
        let key = "ttl_test:string:default";
        cleanup_redis_key(key);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE ttl_str_default (value text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}', ttl '60'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        Spi::run("INSERT INTO ttl_str_default VALUES ('hello_ttl');").unwrap();

        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        let ttl: i64 = redis::cmd("TTL").arg(key).query(&mut conn).unwrap();
        assert!(ttl > 0 && ttl <= 60, "Expected TTL 1-60, got {}", ttl);

        Spi::run("DROP FOREIGN TABLE ttl_str_default;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }

    #[pg_test]
    fn test_ttl_column_override_string() {
        setup_fdw();
        let key = "ttl_test:string:override";
        cleanup_redis_key(key);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE ttl_str_override (value text, ttl bigint) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}', ttl '300'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        // Insert with per-row TTL override of 30 seconds
        Spi::run("INSERT INTO ttl_str_override VALUES ('ttl_override', 30);").unwrap();

        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        let ttl: i64 = redis::cmd("TTL").arg(key).query(&mut conn).unwrap();
        assert!(ttl > 0 && ttl <= 30, "Expected TTL 1-30, got {}", ttl);

        Spi::run("DROP FOREIGN TABLE ttl_str_override;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }

    #[pg_test]
    fn test_ttl_persist_via_column() {
        setup_fdw();
        let key = "ttl_test:string:persist";
        cleanup_redis_key(key);

        // First set with TTL
        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg(key)
            .arg("initial")
            .query(&mut conn)
            .unwrap();
        let _: () = redis::cmd("EXPIRE")
            .arg(key)
            .arg(120)
            .query(&mut conn)
            .unwrap();

        Spi::run(&format!(
            "CREATE FOREIGN TABLE ttl_str_persist (value text, ttl bigint) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        // Update with TTL = -1 to persist
        Spi::run("UPDATE ttl_str_persist SET value = 'updated', ttl = -1;").unwrap();

        let ttl: i64 = redis::cmd("TTL").arg(key).query(&mut conn).unwrap();
        assert_eq!(ttl, -1, "Expected persistent key (TTL -1), got {}", ttl);

        Spi::run("DROP FOREIGN TABLE ttl_str_persist;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }

    #[pg_test]
    fn test_ttl_read_in_select() {
        setup_fdw();
        let key = "ttl_test:string:read";
        cleanup_redis_key(key);

        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg(key)
            .arg("readtest")
            .query(&mut conn)
            .unwrap();
        let _: () = redis::cmd("EXPIRE")
            .arg(key)
            .arg(500)
            .query(&mut conn)
            .unwrap();

        Spi::run(&format!(
            "CREATE FOREIGN TABLE ttl_str_read (value text, ttl bigint) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        let ttl_val = Spi::get_one::<String>("SELECT ttl::text FROM ttl_str_read;").unwrap();
        let ttl: i64 = ttl_val.unwrap().parse().unwrap();
        assert!(
            ttl > 0 && ttl <= 500,
            "Expected TTL in range 1-500, got {}",
            ttl
        );

        Spi::run("DROP FOREIGN TABLE ttl_str_read;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }

    #[pg_test]
    fn test_ttl_default_hash_table() {
        setup_fdw();
        let key = "ttl_test:hash:default";
        cleanup_redis_key(key);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE ttl_hash_default (field text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix '{}', ttl '120'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        Spi::run("INSERT INTO ttl_hash_default VALUES ('myfield', 'myvalue');").unwrap();

        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        let ttl: i64 = redis::cmd("TTL").arg(key).query(&mut conn).unwrap();
        assert!(ttl > 0 && ttl <= 120, "Expected TTL 1-120, got {}", ttl);

        Spi::run("DROP FOREIGN TABLE ttl_hash_default;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }

    #[pg_test]
    fn test_ttl_no_default_no_column_stays_persistent() {
        setup_fdw();
        let key = "ttl_test:string:no_ttl";
        cleanup_redis_key(key);

        Spi::run(&format!(
            "CREATE FOREIGN TABLE ttl_str_no_ttl (value text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        Spi::run("INSERT INTO ttl_str_no_ttl VALUES ('persist_forever');").unwrap();

        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        let ttl: i64 = redis::cmd("TTL").arg(key).query(&mut conn).unwrap();
        assert_eq!(ttl, -1, "Expected no TTL (-1), got {}", ttl);

        Spi::run("DROP FOREIGN TABLE ttl_str_no_ttl;").unwrap();
        cleanup_redis_key(key);
        cleanup();
    }
}
