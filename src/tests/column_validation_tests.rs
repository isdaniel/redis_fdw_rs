#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    const REDIS_HOST_PORT: &str = "127.0.0.1:8899";
    const TEST_DATABASE: &str = "15";
    const FDW_NAME: &str = "redis_colval_fdw";
    const SERVER_NAME: &str = "redis_colval_server";

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

    fn cleanup_redis_key(key: &str) {
        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        let _: Result<(), _> = redis::cmd("DEL").arg(key).query(&mut conn);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // String type: exactly 1 data column
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'string' requires exactly 1 data column")]
    fn test_string_rejects_too_many_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colval_str_bad (value text, extra text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix 'colval:str:bad'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM colval_str_bad;").unwrap();
    }

    #[pg_test]
    fn test_string_accepts_one_column() {
        setup_fdw();
        let key = "colval:str:ok";
        cleanup_redis_key(key);
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colval_str_ok (value text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();
        Spi::run("SELECT * FROM colval_str_ok;").unwrap();
    }

    #[pg_test]
    fn test_string_with_ttl_is_valid() {
        setup_fdw();
        let key = "colval:str:ttl";
        cleanup_redis_key(key);
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colval_str_ttl (value text, ttl bigint) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}', ttl '3600'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();
        Spi::run("SELECT * FROM colval_str_ttl;").unwrap();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Hash type: exactly 2 data columns
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'hash' requires exactly 2 data column")]
    fn test_hash_rejects_too_many_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colval_hash_bad (field text, value text, extra1 text, extra2 text, extra3 text) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix 'colval:hash:bad'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM colval_hash_bad;").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'hash' requires exactly 2 data column")]
    fn test_hash_rejects_too_few_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colval_hash_few (field text) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix 'colval:hash:few'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM colval_hash_few;").unwrap();
    }

    #[pg_test]
    fn test_hash_accepts_exactly_two_columns() {
        setup_fdw();
        let key = "colval:hash:ok";
        cleanup_redis_key(key);
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colval_hash_ok (field text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();
        Spi::run("SELECT * FROM colval_hash_ok;").unwrap();
    }

    #[pg_test]
    fn test_hash_with_ttl_is_valid() {
        setup_fdw();
        let key = "colval:hash:ttl";
        cleanup_redis_key(key);
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colval_hash_ttl (field text, value text, ttl bigint) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix '{}', ttl '300'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();
        Spi::run("SELECT * FROM colval_hash_ttl;").unwrap();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Set type: exactly 1 data column
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'set' requires exactly 1 data column")]
    fn test_set_rejects_two_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colval_set_bad (member text, extra text) SERVER {} OPTIONS (
                database '{}', table_type 'set', table_key_prefix 'colval:set:bad'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM colval_set_bad;").unwrap();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ZSet type: exactly 2 data columns
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'zset' requires exactly 2 data column")]
    fn test_zset_rejects_three_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colval_zset_bad (member text, score float8, extra text) SERVER {} OPTIONS (
                database '{}', table_type 'zset', table_key_prefix 'colval:zset:bad'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM colval_zset_bad;").unwrap();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // List type: 1 or 2 data columns
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    fn test_list_accepts_one_column() {
        setup_fdw();
        let key = "colval:list:one";
        cleanup_redis_key(key);
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colval_list_one (element text) SERVER {} OPTIONS (
                database '{}', table_type 'list', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();
        Spi::run("SELECT * FROM colval_list_one;").unwrap();
    }

    #[pg_test]
    fn test_list_accepts_two_columns() {
        setup_fdw();
        let key = "colval:list:two";
        cleanup_redis_key(key);
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colval_list_two (idx int, element text) SERVER {} OPTIONS (
                database '{}', table_type 'list', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();
        Spi::run("SELECT * FROM colval_list_two;").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'list' requires 1-2 data column")]
    fn test_list_rejects_three_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colval_list_bad (idx int, element text, extra text) SERVER {} OPTIONS (
                database '{}', table_type 'list', table_key_prefix 'colval:list:bad'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM colval_list_bad;").unwrap();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Stream type: 2+ columns (no max)
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    fn test_stream_accepts_many_columns() {
        setup_fdw();
        let key = "colval:stream:many";
        cleanup_redis_key(key);
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colval_stream_many (stream_id text, f1 text, f2 text, f3 text, f4 text) SERVER {} OPTIONS (
                database '{}', table_type 'stream', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();
        Spi::run("SELECT * FROM colval_stream_many;").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'stream' requires 2")]
    fn test_stream_rejects_one_column() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colval_stream_bad (stream_id text) SERVER {} OPTIONS (
                database '{}', table_type 'stream', table_key_prefix 'colval:stream:bad'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM colval_stream_bad;").unwrap();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Multi-key mode: adds +1 for key column
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    fn test_multi_key_allows_extra_key_column() {
        setup_fdw();
        let key = "colval:multi:*";
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colval_multi_ok (key text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();
        Spi::run("SELECT * FROM colval_multi_ok;").unwrap();
    }

    #[pg_test]
    fn test_multi_key_hash_three_columns() {
        setup_fdw();
        let key = "colval:multih:*";
        Spi::run(&format!(
            "CREATE FOREIGN TABLE colval_multih_ok (key text, field text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();
        Spi::run("SELECT * FROM colval_multih_ok;").unwrap();
    }
}
