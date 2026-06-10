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

    // --- helpers for classifier integration tests ---

    fn ensure_server() {
        // Idempotent: only create FDW/server if not already present.
        let _ = Spi::run(
            "DO $$ BEGIN \
               IF NOT EXISTS (SELECT 1 FROM pg_foreign_data_wrapper WHERE fdwname = 'explain_test_wrap') THEN \
                 CREATE FOREIGN DATA WRAPPER explain_test_wrap \
                   HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator; \
               END IF; \
               IF NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = 'explain_test_srv') THEN \
                 CREATE SERVER explain_test_srv FOREIGN DATA WRAPPER explain_test_wrap \
                   OPTIONS (host_port '127.0.0.1:8899'); \
               END IF; \
             END $$;",
        );
    }

    fn setup_zset_for_explain(table: &str) {
        ensure_server();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {table};"));
        Spi::run(&format!(
            "CREATE FOREIGN TABLE {table} (member text, score float8) \
             SERVER explain_test_srv \
             OPTIONS (database '15', table_type 'zset', \
                      table_key_prefix 'explain_test:{table}');"
        ))
        .expect("create zset table");
    }

    fn setup_hash_for_explain(table: &str) {
        ensure_server();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {table};"));
        Spi::run(&format!(
            "CREATE FOREIGN TABLE {table} (field text, value text) \
             SERVER explain_test_srv \
             OPTIONS (database '15', table_type 'hash', \
                      table_key_prefix 'explain_test:{table}');"
        ))
        .expect("create hash table");
    }

    fn setup_set_for_explain(table: &str) {
        ensure_server();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {table};"));
        Spi::run(&format!(
            "CREATE FOREIGN TABLE {table} (member text) \
             SERVER explain_test_srv \
             OPTIONS (database '15', table_type 'set', \
                      table_key_prefix 'explain_test:{table}');"
        ))
        .expect("create set table");
    }

    fn teardown_explain_table(table: &str) {
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {table};"));
    }

    #[pg_test]
    fn explain_zset_score_range_shows_zrangebyscore() {
        setup_zset_for_explain("explain_zs_range");
        let plan = explain_text("SELECT * FROM explain_zs_range WHERE score >= 10 AND score <= 20");
        assert!(
            plan.contains("ZRANGEBYSCORE"),
            "expected ZRANGEBYSCORE in EXPLAIN, got:\n{plan}"
        );
        teardown_explain_table("explain_zs_range");
    }

    #[pg_test]
    fn explain_zset_member_eq_shows_zscore() {
        setup_zset_for_explain("explain_zs_eq");
        let plan = explain_text("SELECT * FROM explain_zs_eq WHERE member = 'alice'");
        let ops_line = plan
            .lines()
            .find(|l| l.contains("Redis Ops:"))
            .expect("Redis Ops line missing");
        assert!(
            ops_line.contains("ZSCORE") && !ops_line.contains("ZRANGEBYSCORE"),
            "expected ZSCORE (not ZRANGEBYSCORE) in Redis Ops line, got: {}",
            ops_line
        );
        teardown_explain_table("explain_zs_eq");
    }

    #[pg_test]
    fn explain_hash_eq_shows_hget() {
        setup_hash_for_explain("explain_h_eq");
        let plan = explain_text("SELECT * FROM explain_h_eq WHERE field = 'name'");
        let ops_line = plan
            .lines()
            .find(|l| l.contains("Redis Ops:"))
            .unwrap_or("");
        assert!(
            ops_line.contains("HGET") && !ops_line.contains("HGETALL"),
            "expected HGET (not HGETALL) in Redis Ops, got:\n{plan}"
        );
        teardown_explain_table("explain_h_eq");
    }

    #[pg_test]
    fn explain_set_in_shows_smismember() {
        setup_set_for_explain("explain_s_in");
        let plan = explain_text("SELECT * FROM explain_s_in WHERE member IN ('a','b','c')");
        assert!(
            plan.contains("SMISMEMBER"),
            "expected SMISMEMBER in EXPLAIN, got:\n{plan}"
        );
        teardown_explain_table("explain_s_in");
    }

    #[pg_test]
    fn explain_hash_no_pushdown_shows_hgetall() {
        setup_hash_for_explain("explain_h_none");
        let plan = explain_text("SELECT * FROM explain_h_none");
        assert!(
            plan.contains("HGETALL"),
            "expected HGETALL in EXPLAIN, got:\n{plan}"
        );
        teardown_explain_table("explain_h_none");
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
