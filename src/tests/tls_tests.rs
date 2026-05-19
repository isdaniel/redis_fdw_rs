#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn run_validator_test(test_name: &str, host_port: &str) {
        let fdw_name = format!("redis_tls_fdw_{}", test_name);
        let server_name = format!("redis_tls_server_{}", test_name);

        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            fdw_name
        ));
        Spi::run(&format!(
            "CREATE FOREIGN DATA WRAPPER {} HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;",
            fdw_name
        ))
        .unwrap();

        Spi::run(&format!(
            "CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (host_port '{}');",
            server_name, fdw_name, host_port
        ))
        .unwrap();

        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            fdw_name
        ));
    }

    #[pg_test]
    fn test_validator_accepts_rediss_scheme() {
        run_validator_test("rediss_scheme", "rediss://redis.example.com:6380");
    }

    #[pg_test]
    fn test_validator_accepts_rediss_insecure() {
        run_validator_test(
            "rediss_insecure",
            "rediss://redis-dev.internal:6380/#insecure",
        );
    }

    #[pg_test]
    fn test_validator_accepts_rediss_cluster() {
        run_validator_test("rediss_cluster", "rediss://node1:6380,rediss://node2:6380");
    }
}
