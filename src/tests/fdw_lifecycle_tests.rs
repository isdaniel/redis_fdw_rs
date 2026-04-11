/// FDW Lifecycle Integration Tests
///
/// Comprehensive tests that exercise the full Foreign Data Wrapper lifecycle:
///   CREATE FOREIGN TABLE -> INSERT -> SELECT (with pushdown) -> DELETE -> DROP
///
/// Each test covers a specific Redis table type with thorough WHERE clause testing
/// including equality (=), IN, LIKE, and LIMIT/OFFSET pushdown.
///
/// Prerequisites:
///   - Redis server running on 127.0.0.1:8899
///   - Database 15 available for testing

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    const REDIS_HOST_PORT: &str = "127.0.0.1:8899";
    const TEST_DATABASE: &str = "15";
    const FDW_NAME: &str = "lifecycle_test_wrapper";
    const SERVER_NAME: &str = "lifecycle_test_server";

    // ── helpers ──────────────────────────────────────────────────────────

    fn setup() {
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            FDW_NAME
        ));
        Spi::run(&format!(
            "CREATE FOREIGN DATA WRAPPER {} HANDLER redis_fdw_handler;",
            FDW_NAME
        ))
        .unwrap();
        Spi::run(&format!(
            "CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (host_port '{}');",
            SERVER_NAME, FDW_NAME, REDIS_HOST_PORT
        ))
        .unwrap();
    }

    fn teardown() {
        let _ = Spi::run(&format!("DROP SERVER IF EXISTS {} CASCADE;", SERVER_NAME));
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            FDW_NAME
        ));
    }

    fn create_table(name: &str, cols: &str, ttype: &str, key: &str) {
        Spi::run(&format!(
            "CREATE FOREIGN TABLE {name} ({cols}) SERVER {SERVER_NAME} OPTIONS (\
                database '{TEST_DATABASE}', table_type '{ttype}', table_key_prefix '{key}');"
        ))
        .unwrap();
    }

    fn drop_table(name: &str) {
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {name};"));
    }

    fn count(table: &str) -> i64 {
        Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {table};"))
            .unwrap()
            .unwrap()
    }

    fn count_where(table: &str, cond: &str) -> i64 {
        Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {table} WHERE {cond};"))
            .unwrap()
            .unwrap()
    }

    fn get_one_text(sql: &str) -> String {
        Spi::get_one::<String>(sql).unwrap().unwrap()
    }

    // ── String table lifecycle ──────────────────────────────────────────

    #[pg_test]
    fn test_lifecycle_string_create_select_delete_drop() {
        setup();
        let t = "lc_string";
        let k = "lifecycle:string:basic";
        create_table(t, "value TEXT", "string", k);

        // INSERT
        Spi::run(&format!("INSERT INTO {t} (value) VALUES ('hello world');")).unwrap();

        // SELECT full
        assert_eq!(count(t), 1);
        assert_eq!(get_one_text(&format!("SELECT value FROM {t};")), "hello world");

        // SELECT with WHERE = pushdown
        assert_eq!(count_where(t, "value = 'hello world'"), 1);
        assert_eq!(count_where(t, "value = 'nonexistent'"), 0);

        // SELECT with WHERE LIKE pushdown
        assert_eq!(count_where(t, "value LIKE 'hello%'"), 1);
        assert_eq!(count_where(t, "value LIKE 'goodbye%'"), 0);

        // SELECT with OFFSET beyond data
        let offset_result = Spi::get_one::<String>(&format!(
            "SELECT value FROM {t} OFFSET 1;"
        ));
        // Should return no rows
        assert!(offset_result.is_err() || offset_result.unwrap().is_none());

        // DELETE (string -> DEL key)
        Spi::run(&format!("DELETE FROM {t} WHERE value = 'hello world';")).unwrap();
        assert_eq!(count(t), 0);

        // DROP
        drop_table(t);
        teardown();
    }

    // ── Hash table lifecycle ────────────────────────────────────────────

    #[pg_test]
    fn test_lifecycle_hash_create_select_delete_drop() {
        setup();
        let t = "lc_hash";
        let k = "lifecycle:hash:basic";
        create_table(t, "field TEXT, value TEXT", "hash", k);

        // INSERT multiple fields
        Spi::run(&format!("INSERT INTO {t} VALUES ('name','Alice');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} VALUES ('email','alice@test.com');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} VALUES ('role','admin');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} VALUES ('dept','engineering');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} VALUES ('loc','new_york');")).unwrap();

        // SELECT full
        assert_eq!(count(t), 5);

        // SELECT with WHERE field = (HGET pushdown)
        assert_eq!(
            get_one_text(&format!("SELECT value FROM {t} WHERE field = 'email';")),
            "alice@test.com"
        );

        // SELECT with WHERE field IN (HMGET pushdown)
        assert_eq!(count_where(t, "field IN ('name','role')"), 2);

        // SELECT with WHERE field LIKE (HSCAN pushdown)
        // Fields starting with 'e' -> email
        assert_eq!(count_where(t, "field LIKE 'e%'"), 1);
        // Fields containing 'e': name, email, role, dept (4 of 5)
        assert_eq!(count_where(t, "field LIKE '%e%'"), 4);

        // DELETE specific field
        Spi::run(&format!("DELETE FROM {t} WHERE field = 'role';")).unwrap();
        assert_eq!(count(t), 4);
        assert_eq!(count_where(t, "field = 'role'"), 0);

        // DELETE another field
        Spi::run(&format!("DELETE FROM {t} WHERE field = 'loc';")).unwrap();
        assert_eq!(count(t), 3);

        // DROP
        drop_table(t);
        teardown();
    }

    // ── List table lifecycle ────────────────────────────────────────────

    #[pg_test]
    fn test_lifecycle_list_create_select_delete_drop() {
        setup();
        let t = "lc_list";
        let k = "lifecycle:list:basic";
        create_table(t, "element TEXT", "list", k);

        // Clean up any prior data
        Spi::run(&format!("DELETE FROM {t};")).unwrap();

        // INSERT multiple elements
        let items = ["alpha", "beta", "gamma", "delta", "epsilon"];
        for item in &items {
            Spi::run(&format!("INSERT INTO {t} (element) VALUES ('{item}');")).unwrap();
        }

        // SELECT full
        assert_eq!(count(t), 5);

        // SELECT with WHERE = pushdown (client-side filter on lists)
        assert_eq!(count_where(t, "element = 'gamma'"), 1);
        assert_eq!(count_where(t, "element = 'nonexistent'"), 0);

        // SELECT with WHERE LIKE pushdown
        // alpha, beta, gamma, delta contain 'a'; epsilon does not
        assert_eq!(count_where(t, "element LIKE '%a%'"), 4);

        // SELECT with LIMIT (direct — verified by constraining results)
        let first_elem = Spi::get_one::<String>(&format!(
            "SELECT element FROM {t} LIMIT 1;"
        ));
        assert!(first_elem.is_ok());
        assert!(first_elem.unwrap().is_some());

        // DELETE specific element
        Spi::run(&format!("DELETE FROM {t} WHERE element = 'beta';")).unwrap();
        assert_eq!(count(t), 4);
        assert_eq!(count_where(t, "element = 'beta'"), 0);

        // DROP
        drop_table(t);
        teardown();
    }

    // ── Set table lifecycle ─────────────────────────────────────────────

    #[pg_test]
    fn test_lifecycle_set_create_select_delete_drop() {
        setup();
        let t = "lc_set";
        let k = "lifecycle:set:basic";
        create_table(t, "member TEXT", "set", k);

        // INSERT members
        let members = ["rust", "go", "python", "java", "ruby"];
        for m in &members {
            Spi::run(&format!("INSERT INTO {t} (member) VALUES ('{m}');")).unwrap();
        }

        // SELECT full
        assert_eq!(count(t), 5);

        // Test set uniqueness - duplicate INSERT should not increase count
        Spi::run(&format!("INSERT INTO {t} (member) VALUES ('rust');")).unwrap();
        assert_eq!(count(t), 5);

        // SELECT with WHERE = pushdown (SISMEMBER)
        assert_eq!(count_where(t, "member = 'rust'"), 1);
        assert_eq!(count_where(t, "member = 'haskell'"), 0);

        // SELECT with WHERE IN pushdown (SMISMEMBER)
        assert_eq!(count_where(t, "member IN ('rust','python','haskell')"), 2);

        // SELECT with WHERE LIKE pushdown (SSCAN)
        assert_eq!(count_where(t, "member LIKE 'r%'"), 2); // rust, ruby

        // DELETE specific member (SREM)
        Spi::run(&format!("DELETE FROM {t} WHERE member = 'java';")).unwrap();
        assert_eq!(count(t), 4);
        assert_eq!(count_where(t, "member = 'java'"), 0);

        // DELETE another member
        Spi::run(&format!("DELETE FROM {t} WHERE member = 'go';")).unwrap();
        assert_eq!(count(t), 3);

        // DROP
        drop_table(t);
        teardown();
    }

    // ── ZSet table lifecycle ────────────────────────────────────────────

    #[pg_test]
    fn test_lifecycle_zset_create_select_delete_drop() {
        setup();
        let t = "lc_zset";
        let k = "lifecycle:zset:basic";
        create_table(t, "member TEXT, score FLOAT8", "zset", k);

        // INSERT scored members
        Spi::run(&format!("INSERT INTO {t} VALUES ('alice', 100);")).unwrap();
        Spi::run(&format!("INSERT INTO {t} VALUES ('bob', 200);")).unwrap();
        Spi::run(&format!("INSERT INTO {t} VALUES ('charlie', 300);")).unwrap();
        Spi::run(&format!("INSERT INTO {t} VALUES ('diana', 400);")).unwrap();
        Spi::run(&format!("INSERT INTO {t} VALUES ('eve', 500);")).unwrap();

        // SELECT full
        assert_eq!(count(t), 5);

        // SELECT with WHERE member = pushdown (ZSCORE)
        assert_eq!(count_where(t, "member = 'bob'"), 1);
        let bob_score = Spi::get_one::<f64>(&format!(
            "SELECT score FROM {t} WHERE member = 'bob';"
        ))
        .unwrap()
        .unwrap();
        assert!((bob_score - 200.0).abs() < 0.01);

        // SELECT with WHERE member IN pushdown (ZMSCORE)
        assert_eq!(count_where(t, "member IN ('alice','charlie','zara')"), 2);

        // SELECT with WHERE member LIKE pushdown (ZSCAN)
        assert_eq!(count_where(t, "member LIKE 'a%'"), 1); // alice
        // Members containing 'e': alice, charlie, eve = 3
        assert_eq!(count_where(t, "member LIKE '%e%'"), 3);

        // SELECT with score filter (NOT pushed down to Redis - handled by PG post-filter)
        assert_eq!(count_where(t, "score > 250"), 3); // charlie, diana, eve
        assert_eq!(count_where(t, "score = 300"), 1); // charlie

        // SELECT with LIMIT (direct query, not subquery)
        let first_member = Spi::get_one::<String>(&format!(
            "SELECT member FROM {t} LIMIT 1;"
        ));
        assert!(first_member.is_ok());
        assert!(first_member.unwrap().is_some());

        // DELETE specific member (ZREM)
        Spi::run(&format!("DELETE FROM {t} WHERE member = 'diana';")).unwrap();
        assert_eq!(count(t), 4);
        assert_eq!(count_where(t, "member = 'diana'"), 0);

        // DROP
        drop_table(t);
        teardown();
    }

    // ── Stream table lifecycle ──────────────────────────────────────────

    #[pg_test]
    fn test_lifecycle_stream_create_select_drop() {
        setup();
        let t = "lc_stream";
        let k = "lifecycle:stream:basic";
        create_table(t, "id TEXT, field TEXT, value TEXT", "stream", k);

        // INSERT stream entries
        Spi::run(&format!(
            "INSERT INTO {t} (id, field, value) VALUES ('*', 'sensor', 'temp');"
        ))
        .unwrap();
        Spi::run(&format!(
            "INSERT INTO {t} (id, field, value) VALUES ('*', 'sensor', 'humidity');"
        ))
        .unwrap();
        Spi::run(&format!(
            "INSERT INTO {t} (id, field, value) VALUES ('*', 'status', 'active');"
        ))
        .unwrap();

        // SELECT full - should have entries
        let c = count(t);
        assert!(c >= 3, "Expected at least 3 stream entries, got {c}");

        // SELECT with LIMIT
        let first_entry = Spi::get_one::<String>(&format!(
            "SELECT id FROM {t} LIMIT 1;"
        ));
        assert!(first_entry.is_ok());
        assert!(first_entry.unwrap().is_some());

        // DROP (streams don't support DELETE through FDW the same way)
        drop_table(t);
        teardown();
    }

    // ── Cross-type lifecycle in single test ──────────────────────────────

    #[pg_test]
    fn test_lifecycle_all_types_create_operate_drop() {
        setup();

        // Create all table types at once
        create_table("lc_all_str", "value TEXT", "string", "lifecycle:all:string");
        create_table(
            "lc_all_hash",
            "field TEXT, value TEXT",
            "hash",
            "lifecycle:all:hash",
        );
        create_table("lc_all_list", "element TEXT", "list", "lifecycle:all:list");
        create_table("lc_all_set", "member TEXT", "set", "lifecycle:all:set");
        create_table(
            "lc_all_zset",
            "member TEXT, score FLOAT8",
            "zset",
            "lifecycle:all:zset",
        );

        // Clean list to avoid stale data
        Spi::run("DELETE FROM lc_all_list;").unwrap();

        // INSERT into each
        Spi::run("INSERT INTO lc_all_str (value) VALUES ('test_string');").unwrap();
        Spi::run("INSERT INTO lc_all_hash VALUES ('k1','v1');").unwrap();
        Spi::run("INSERT INTO lc_all_hash VALUES ('k2','v2');").unwrap();
        Spi::run("INSERT INTO lc_all_list (element) VALUES ('item1');").unwrap();
        Spi::run("INSERT INTO lc_all_list (element) VALUES ('item2');").unwrap();
        Spi::run("INSERT INTO lc_all_set (member) VALUES ('m1');").unwrap();
        Spi::run("INSERT INTO lc_all_set (member) VALUES ('m2');").unwrap();
        Spi::run("INSERT INTO lc_all_set (member) VALUES ('m3');").unwrap();
        Spi::run("INSERT INTO lc_all_zset VALUES ('z1', 10);").unwrap();
        Spi::run("INSERT INTO lc_all_zset VALUES ('z2', 20);").unwrap();

        // Verify counts
        assert_eq!(count("lc_all_str"), 1);
        assert_eq!(count("lc_all_hash"), 2);
        assert_eq!(count("lc_all_list"), 2);
        assert_eq!(count("lc_all_set"), 3);
        assert_eq!(count("lc_all_zset"), 2);

        // Pushdown SELECT on each type
        assert_eq!(count_where("lc_all_str", "value = 'test_string'"), 1);
        assert_eq!(
            get_one_text("SELECT value FROM lc_all_hash WHERE field = 'k1';"),
            "v1"
        );
        assert_eq!(count_where("lc_all_list", "element = 'item1'"), 1);
        assert_eq!(count_where("lc_all_set", "member = 'm2'"), 1);
        assert_eq!(count_where("lc_all_zset", "member = 'z1'"), 1);

        // DELETE from each type
        Spi::run("DELETE FROM lc_all_str WHERE value = 'test_string';").unwrap();
        Spi::run("DELETE FROM lc_all_hash WHERE field = 'k1';").unwrap();
        Spi::run("DELETE FROM lc_all_list WHERE element = 'item1';").unwrap();
        Spi::run("DELETE FROM lc_all_set WHERE member = 'm1';").unwrap();
        Spi::run("DELETE FROM lc_all_zset WHERE member = 'z1';").unwrap();

        // Verify deletes
        assert_eq!(count("lc_all_str"), 0);
        assert_eq!(count("lc_all_hash"), 1);
        assert_eq!(count("lc_all_list"), 1);
        assert_eq!(count("lc_all_set"), 2);
        assert_eq!(count("lc_all_zset"), 1);

        // DROP all
        drop_table("lc_all_str");
        drop_table("lc_all_hash");
        drop_table("lc_all_list");
        drop_table("lc_all_set");
        drop_table("lc_all_zset");
        teardown();
    }

    // ── FDW setup / teardown lifecycle ───────────────────────────────────

    #[pg_test]
    fn test_lifecycle_fdw_create_and_drop() {
        // Verify we can create, use, and cleanly drop the full FDW stack
        setup();

        let t = "lc_fdw_drop_tbl";
        create_table(t, "value TEXT", "string", "lifecycle:fdw:drop");

        Spi::run(&format!("INSERT INTO {t} (value) VALUES ('before_drop');")).unwrap();
        assert_eq!(count(t), 1);

        // DROP the table explicitly
        drop_table(t);

        // Verify the table no longer exists by checking pg_class catalog
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = 'lc_fdw_drop_tbl');"
        )
        .unwrap()
        .unwrap();
        assert!(!exists, "Table should not exist in pg_class after DROP");

        // Now DROP CASCADE the entire FDW stack
        Spi::run(&format!("DROP FOREIGN DATA WRAPPER {FDW_NAME} CASCADE;")).unwrap();

        // Verify the server is also gone via catalog check
        let server_exists = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS(SELECT 1 FROM pg_foreign_server WHERE srvname = '{SERVER_NAME}');"
        ))
        .unwrap()
        .unwrap();
        assert!(
            !server_exists,
            "Server should not exist after DROP CASCADE"
        );

        // Verify the FDW is also gone
        let fdw_exists = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS(SELECT 1 FROM pg_foreign_data_wrapper WHERE fdwname = '{FDW_NAME}');"
        ))
        .unwrap()
        .unwrap();
        assert!(!fdw_exists, "FDW should not exist after DROP CASCADE");
    }

    // ── Edge cases ──────────────────────────────────────────────────────

    #[pg_test]
    fn test_lifecycle_empty_tables() {
        setup();

        create_table("lc_empty_hash", "field TEXT, value TEXT", "hash", "lifecycle:empty:hash");
        create_table("lc_empty_set", "member TEXT", "set", "lifecycle:empty:set");
        create_table("lc_empty_zset", "member TEXT, score FLOAT8", "zset", "lifecycle:empty:zset");

        // SELECT on empty tables should return 0 rows
        assert_eq!(count("lc_empty_hash"), 0);
        assert_eq!(count("lc_empty_set"), 0);
        assert_eq!(count("lc_empty_zset"), 0);

        // Pushdown on empty tables should work without error
        assert_eq!(count_where("lc_empty_hash", "field = 'nothing'"), 0);
        assert_eq!(count_where("lc_empty_set", "member = 'nothing'"), 0);
        assert_eq!(count_where("lc_empty_zset", "member = 'nothing'"), 0);

        drop_table("lc_empty_hash");
        drop_table("lc_empty_set");
        drop_table("lc_empty_zset");
        teardown();
    }

    #[pg_test]
    fn test_lifecycle_special_characters() {
        setup();
        let t = "lc_special";
        let k = "lifecycle:special:hash";
        create_table(t, "field TEXT, value TEXT", "hash", k);

        // INSERT values with special characters
        Spi::run(&format!(
            "INSERT INTO {t} VALUES ('key1', 'value with spaces');"
        ))
        .unwrap();
        Spi::run(&format!(
            "INSERT INTO {t} VALUES ('key2', 'value:with:colons');"
        ))
        .unwrap();
        Spi::run(&format!(
            "INSERT INTO {t} VALUES ('key3', 'value-with-dashes');"
        ))
        .unwrap();

        assert_eq!(count(t), 3);

        // Pushdown with special char values
        assert_eq!(
            get_one_text(&format!("SELECT value FROM {t} WHERE field = 'key1';")),
            "value with spaces"
        );
        assert_eq!(
            get_one_text(&format!("SELECT value FROM {t} WHERE field = 'key2';")),
            "value:with:colons"
        );

        drop_table(t);
        teardown();
    }

    #[pg_test]
    fn test_lifecycle_zset_score_filtering() {
        setup();
        let t = "lc_zset_score";
        let k = "lifecycle:zset:score_filter";
        create_table(t, "member TEXT, score FLOAT8", "zset", k);

        Spi::run(&format!("INSERT INTO {t} VALUES ('low', 10);")).unwrap();
        Spi::run(&format!("INSERT INTO {t} VALUES ('mid', 50);")).unwrap();
        Spi::run(&format!("INSERT INTO {t} VALUES ('high', 90);")).unwrap();

        // Score filters should work via PG post-filter (not pushed to Redis)
        assert_eq!(count_where(t, "score > 40"), 2);
        assert_eq!(count_where(t, "score < 40"), 1);
        assert_eq!(count_where(t, "score >= 10 AND score <= 50"), 2);
        assert_eq!(count_where(t, "score = 50"), 1);
        assert_eq!(
            get_one_text(&format!("SELECT member FROM {t} WHERE score = 90;")),
            "high"
        );

        // Combined member pushdown + score post-filter
        assert_eq!(count_where(t, "member = 'mid' AND score = 50"), 1);
        assert_eq!(count_where(t, "member = 'mid' AND score = 99"), 0);

        drop_table(t);
        teardown();
    }

    #[pg_test]
    fn test_lifecycle_hash_update_via_insert() {
        setup();
        let t = "lc_hash_update";
        let k = "lifecycle:hash:update";
        create_table(t, "field TEXT, value TEXT", "hash", k);

        // INSERT initial value
        Spi::run(&format!("INSERT INTO {t} VALUES ('counter', '1');")).unwrap();
        assert_eq!(
            get_one_text(&format!("SELECT value FROM {t} WHERE field = 'counter';")),
            "1"
        );

        // INSERT same field again - Redis HSET overwrites the value
        Spi::run(&format!("INSERT INTO {t} VALUES ('counter', '42');")).unwrap();
        assert_eq!(
            get_one_text(&format!("SELECT value FROM {t} WHERE field = 'counter';")),
            "42"
        );

        // Count should still be 1 (same field, overwritten)
        assert_eq!(count(t), 1);

        drop_table(t);
        teardown();
    }

    #[pg_test]
    fn test_lifecycle_zset_score_update() {
        setup();
        let t = "lc_zset_update";
        let k = "lifecycle:zset:update";
        create_table(t, "member TEXT, score FLOAT8", "zset", k);

        // INSERT member with initial score
        Spi::run(&format!("INSERT INTO {t} VALUES ('player', 100);")).unwrap();
        let score = Spi::get_one::<f64>(&format!(
            "SELECT score FROM {t} WHERE member = 'player';"
        ))
        .unwrap()
        .unwrap();
        assert!((score - 100.0).abs() < 0.01);

        // INSERT same member with new score - ZADD updates score
        Spi::run(&format!("INSERT INTO {t} VALUES ('player', 999);")).unwrap();
        let new_score = Spi::get_one::<f64>(&format!(
            "SELECT score FROM {t} WHERE member = 'player';"
        ))
        .unwrap()
        .unwrap();
        assert!((new_score - 999.0).abs() < 0.01);

        // Still only 1 member
        assert_eq!(count(t), 1);

        drop_table(t);
        teardown();
    }

    #[pg_test]
    fn test_lifecycle_list_duplicate_elements() {
        setup();
        let t = "lc_list_dup";
        let k = "lifecycle:list:dup";
        create_table(t, "element TEXT", "list", k);

        Spi::run(&format!("DELETE FROM {t};")).unwrap();

        // Lists allow duplicates
        Spi::run(&format!("INSERT INTO {t} (element) VALUES ('repeat');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} (element) VALUES ('repeat');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} (element) VALUES ('repeat');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} (element) VALUES ('unique');")).unwrap();

        assert_eq!(count(t), 4);
        assert_eq!(count_where(t, "element = 'repeat'"), 3);
        assert_eq!(count_where(t, "element = 'unique'"), 1);

        // DELETE removes all occurrences (LREM count=0)
        Spi::run(&format!("DELETE FROM {t} WHERE element = 'repeat';")).unwrap();
        assert_eq!(count(t), 1);

        drop_table(t);
        teardown();
    }

    #[pg_test]
    fn test_lifecycle_set_in_with_no_matches() {
        setup();
        let t = "lc_set_in_empty";
        let k = "lifecycle:set:in_empty";
        create_table(t, "member TEXT", "set", k);

        Spi::run(&format!("INSERT INTO {t} (member) VALUES ('x');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} (member) VALUES ('y');")).unwrap();

        // IN with no matching members
        assert_eq!(count_where(t, "member IN ('a','b','c')"), 0);

        // IN with partial matches
        assert_eq!(count_where(t, "member IN ('x','a','b')"), 1);

        drop_table(t);
        teardown();
    }
}
