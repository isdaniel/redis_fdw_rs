//! Integration tests for EXPLAIN output. Asserts label *presence* (not exact
//! line formatting) so the tests remain stable across PG 14-18.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn drop_all() {
        let _ = Spi::run("DROP FOREIGN TABLE IF EXISTS explain_test_hash;");
        let _ = Spi::run("DROP SERVER IF EXISTS explain_test_srv CASCADE;");
        let _ = Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS explain_test_wrap CASCADE;");
    }

    fn setup_hash() {
        drop_all();
        Spi::run(
            "CREATE FOREIGN DATA WRAPPER explain_test_wrap \
             HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;",
        )
        .expect("create fdw");
        Spi::run(
            "CREATE SERVER explain_test_srv FOREIGN DATA WRAPPER explain_test_wrap \
             OPTIONS (host_port '127.0.0.1:8899');",
        )
        .expect("create server");
        Spi::run(
            "CREATE FOREIGN TABLE explain_test_hash (field text, value text) \
             SERVER explain_test_srv \
             OPTIONS (database '15', table_type 'hash', table_key_prefix 'explain_test:h1');",
        )
        .expect("create table");
    }

    fn explain_text(sql: &str) -> String {
        let q = format!("EXPLAIN (FORMAT TEXT) {sql}");
        Spi::connect(|client| {
            let mut out = String::new();
            let result = client.select(&q, None, &[]).unwrap();
            for row in result {
                if let Some(line) = row.get::<&str>(1).unwrap() {
                    out.push_str(line);
                    out.push('\n');
                }
            }
            out
        })
    }

    #[pg_test]
    fn explain_text_contains_server_key_type_for_hash() {
        setup_hash();
        let out = explain_text("SELECT * FROM explain_test_hash");
        assert!(
            out.contains("Redis Server:"),
            "missing Redis Server label\n{out}"
        );
        assert!(out.contains("Redis Key:"), "missing Redis Key label\n{out}");
        assert!(
            out.contains("Table Type:"),
            "missing Table Type label\n{out}"
        );
        assert!(
            out.contains("hash"),
            "Table Type should mention hash\n{out}"
        );
        assert!(
            out.contains("Batch Size:"),
            "missing Batch Size label\n{out}"
        );
        drop_all();
    }

    #[pg_test]
    fn explain_text_contains_redis_ops_for_hash_scan() {
        setup_hash();
        let out = explain_text("SELECT * FROM explain_test_hash");
        assert!(out.contains("Redis Ops:"), "missing Redis Ops label\n{out}");
        assert!(
            out.contains("HGETALL"),
            "Redis Ops should mention HGETALL\n{out}"
        );
        drop_all();
    }

    #[pg_test]
    fn explain_text_contains_pushdown_when_filtering_on_field() {
        setup_hash();
        let _ = Spi::run("INSERT INTO explain_test_hash VALUES ('a', '1'), ('b', '2');");
        let out = explain_text("SELECT * FROM explain_test_hash WHERE field = 'a'");
        assert!(out.contains("Pushdown:"), "missing Pushdown label\n{out}");
        // Either "none" (if pushdown couldn't fire) or a description containing "field" — both are valid for PR-1.
        let pushdown_line = out.lines().find(|l| l.contains("Pushdown:")).unwrap_or("");
        assert!(
            pushdown_line.contains("none") || pushdown_line.contains("field"),
            "Pushdown line should be 'none' or describe 'field': {pushdown_line}"
        );
        drop_all();
    }

    #[pg_test]
    fn explain_modify_contains_server_key_type() {
        setup_hash();
        let out = explain_text("INSERT INTO explain_test_hash VALUES ('a', '1')");
        assert!(out.contains("Redis Server:"));
        assert!(out.contains("Redis Key:"));
        assert!(out.contains("Table Type:"));
        // Modify should NOT carry Batch Size / Pushdown labels.
        assert!(
            !out.contains("Batch Size:"),
            "Modify must not emit Batch Size"
        );
        assert!(!out.contains("Pushdown:"), "Modify must not emit Pushdown");
        drop_all();
    }
}
