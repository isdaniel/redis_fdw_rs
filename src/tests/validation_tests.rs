#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    const FDW_NAME: &str = "redis_validation_fdw";
    const SERVER_NAME: &str = "redis_validation_server";

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
    }

    fn cleanup() {
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            FDW_NAME
        ));
    }

    fn setup_fdw_with_server() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (host_port '127.0.0.1:8899');",
            SERVER_NAME, FDW_NAME
        ))
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "missing required option \"host_port\"")]
    fn test_validator_rejects_missing_host_port() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE SERVER {} FOREIGN DATA WRAPPER {};",
            SERVER_NAME, FDW_NAME
        ))
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "host_port must be in format")]
    fn test_validator_rejects_invalid_host_port() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (host_port 'no-port');",
            SERVER_NAME, FDW_NAME
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_validator_accepts_valid_server_options() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (host_port '127.0.0.1:6379');",
            SERVER_NAME, FDW_NAME
        ))
        .unwrap();
        cleanup();
    }

    #[pg_test]
    #[should_panic(expected = "cluster_mode must be")]
    fn test_validator_rejects_invalid_cluster_mode() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (host_port '127.0.0.1:6379', cluster_mode 'yes');",
            SERVER_NAME, FDW_NAME
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_validator_accepts_cluster_mode_true() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (host_port '127.0.0.1:6379', cluster_mode 'true');",
            SERVER_NAME, FDW_NAME
        ))
        .unwrap();
        cleanup();
    }

    #[pg_test]
    #[should_panic(expected = "missing required option \"table_type\"")]
    fn test_validator_rejects_missing_table_type() {
        setup_fdw_with_server();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE val_test_tbl (val text) SERVER {} OPTIONS (table_key_prefix 'test:');",
            SERVER_NAME
        ))
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "invalid table_type")]
    fn test_validator_rejects_invalid_table_type() {
        setup_fdw_with_server();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE val_test_tbl2 (val text) SERVER {} OPTIONS (table_type 'invalid', table_key_prefix 'test:');",
            SERVER_NAME
        ))
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "missing required option \"table_key_prefix\"")]
    fn test_validator_rejects_missing_key_prefix() {
        setup_fdw_with_server();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE val_test_tbl3 (val text) SERVER {} OPTIONS (table_type 'string');",
            SERVER_NAME
        ))
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "database must be an integer between 0 and 15")]
    fn test_validator_rejects_invalid_database() {
        setup_fdw_with_server();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE val_test_tbl4 (val text) SERVER {} OPTIONS (table_type 'string', table_key_prefix 'test:', database '99');",
            SERVER_NAME
        ))
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "ttl must be a positive integer or -1")]
    fn test_validator_rejects_invalid_ttl() {
        setup_fdw_with_server();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE val_test_tbl5 (val text) SERVER {} OPTIONS (table_type 'string', table_key_prefix 'test:', ttl '0');",
            SERVER_NAME
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_validator_accepts_valid_ttl() {
        setup_fdw_with_server();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE val_test_tbl6 (val text) SERVER {} OPTIONS (table_type 'string', table_key_prefix 'test:', ttl '3600');",
            SERVER_NAME
        ))
        .unwrap();
        cleanup();
    }

    #[pg_test]
    #[should_panic(expected = "batch_size must be between 100 and 100000")]
    fn test_validator_rejects_invalid_batch_size() {
        setup_fdw_with_server();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE val_test_tbl7 (val text) SERVER {} OPTIONS (table_type 'string', table_key_prefix 'test:', batch_size '50');",
            SERVER_NAME
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_validator_accepts_valid_table_options() {
        setup_fdw_with_server();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE val_test_tbl8 (field text, value text) SERVER {} OPTIONS (table_type 'hash', table_key_prefix 'test:', database '15', ttl '300', batch_size '5000');",
            SERVER_NAME
        ))
        .unwrap();
        cleanup();
    }
}
