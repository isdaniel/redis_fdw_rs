#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    const REDIS_HOST_PORT: &str = "127.0.0.1:8899";
    const TEST_DATABASE: &str = "15";
    const FDW_NAME: &str = "redis_ddl_fdw";
    const SERVER_NAME: &str = "redis_ddl_server";

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

    // ═══════════════════════════════════════════════════════════════════════
    // Column validation: tables with wrong column count are rejected
    // Validation fires at DDL-time via object_access_hook in production,
    // and as a safety net at query-time via begin_foreign_scan/modify.
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'string' requires exactly 1 data column")]
    fn test_ddl_string_rejects_too_many_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_str_bad (val text, extra text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix 'ddl:str:bad'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM ddl_str_bad;").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'hash' requires exactly 2 data column")]
    fn test_ddl_hash_rejects_too_many_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_hash_bad (f text, v text, extra text) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix 'ddl:hash:bad'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM ddl_hash_bad;").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'hash' requires exactly 2 data column")]
    fn test_ddl_hash_rejects_too_few_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_hash_few (f text) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix 'ddl:hash:few'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM ddl_hash_few;").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'set' requires exactly 1 data column")]
    fn test_ddl_set_rejects_too_many_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_set_bad (member text, extra text) SERVER {} OPTIONS (
                database '{}', table_type 'set', table_key_prefix 'ddl:set:bad'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM ddl_set_bad;").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'zset' requires exactly 2 data column")]
    fn test_ddl_zset_rejects_too_many_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_zset_bad (m text, s text, extra text) SERVER {} OPTIONS (
                database '{}', table_type 'zset', table_key_prefix 'ddl:zset:bad'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM ddl_zset_bad;").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'zset' requires exactly 2 data column")]
    fn test_ddl_zset_rejects_too_few_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_zset_few (m text) SERVER {} OPTIONS (
                database '{}', table_type 'zset', table_key_prefix 'ddl:zset:few'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM ddl_zset_few;").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'stream' requires at least 2 data column")]
    fn test_ddl_stream_rejects_too_few_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_stream_bad (id text) SERVER {} OPTIONS (
                database '{}', table_type 'stream', table_key_prefix 'ddl:stream:bad'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM ddl_stream_bad;").unwrap();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Valid tables succeed at CREATE and query
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    fn test_ddl_string_accepts_valid_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_str_ok (val text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix 'ddl:str:ok'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_ddl_hash_accepts_valid_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_hash_ok (k text, v text) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix 'ddl:hash:ok'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_ddl_zset_accepts_valid_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_zset_ok (item text, rank text) SERVER {} OPTIONS (
                database '{}', table_type 'zset', table_key_prefix 'ddl:zset:ok'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_ddl_stream_accepts_multiple_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_stream_ok (id text, user_id text, action text) SERVER {} OPTIONS (
                database '{}', table_type 'stream', table_key_prefix 'ddl:stream:ok'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // TTL column is excluded from count
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    fn test_ddl_string_with_ttl_column_accepted() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_str_ttl (val text, ttl bigint) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix 'ddl:str:ttl', ttl '3600'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_ddl_hash_with_ttl_column_accepted() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_hash_ttl (field text, value text, ttl bigint) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix 'ddl:hash:ttl', ttl '300'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'string' requires exactly 1 data column")]
    fn test_ddl_string_with_ttl_still_validates() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_str_ttl_bad (val text, extra text, ttl bigint) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix 'ddl:str:ttl:bad', ttl '3600'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM ddl_str_ttl_bad;").unwrap();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Multi-key mode adds +1 expected column
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    fn test_ddl_multi_key_string_accepts_two_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_mk_str (key_name text, val text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix 'user:*'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'string' requires exactly 2 data column")]
    fn test_ddl_multi_key_string_rejects_one_column() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_mk_str_bad (val text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix 'user:*'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM ddl_mk_str_bad;").unwrap();
    }

    #[pg_test]
    fn test_ddl_ttl_first_position_hash_accepted() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_ttl_first_hash (ttl bigint, field text, value text) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix 'ddl:ttl:first:hash', ttl '60'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_ddl_ttl_first_position_string_accepted() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_ttl_first_str (ttl bigint, value text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix 'ddl:ttl:first:str', ttl '60'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_ddl_ttl_first_position_zset_accepted() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_ttl_first_zset (ttl bigint, member text, score text) SERVER {} OPTIONS (
                database '{}', table_type 'zset', table_key_prefix 'ddl:ttl:first:zset', ttl '60'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_ddl_ttl_middle_position_stream_accepted() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_ttl_mid_stream (id text, ttl bigint, user_id text, action text) SERVER {} OPTIONS (
                database '{}', table_type 'stream', table_key_prefix 'ddl:ttl:mid:stream', ttl '60'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'hash' requires exactly 2 data column")]
    fn test_ddl_ttl_first_still_validates_extra_columns() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ddl_ttl_first_bad (ttl bigint, f text, v text, extra text) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix 'ddl:ttl:first:bad', ttl '60'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM ddl_ttl_first_bad;").unwrap();
    }

    #[pg_test]
    fn test_hook_does_not_affect_regular_tables() {
        Spi::run("CREATE TABLE ddl_hook_regular_test (a text, b text, c text, d text, e text);")
            .unwrap();
        Spi::run("DROP TABLE ddl_hook_regular_test;").unwrap();
    }

    #[pg_test]
    fn test_hook_does_not_affect_handler_less_fdw() {
        let _ = Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS dummy_fdw CASCADE;");
        Spi::run("CREATE FOREIGN DATA WRAPPER dummy_fdw;").unwrap();
        Spi::run("CREATE SERVER dummy_server FOREIGN DATA WRAPPER dummy_fdw;").unwrap();
        Spi::run(
            "CREATE FOREIGN TABLE ddl_hook_dummy (a text, b text, c text, d text, e text, f text) SERVER dummy_server;",
        )
        .unwrap();
        Spi::run("DROP FOREIGN TABLE ddl_hook_dummy;").unwrap();
        Spi::run("DROP SERVER dummy_server;").unwrap();
        Spi::run("DROP FOREIGN DATA WRAPPER dummy_fdw;").unwrap();
    }
}
