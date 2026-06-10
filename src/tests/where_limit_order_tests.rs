/// Integration tests covering WHERE + LIMIT/OFFSET + ORDER BY combinations
/// across Hash, ZSet, Stream, List, and Set table types.
///
/// Prerequisites:
/// - Redis server on 127.0.0.1:8899
/// - Database 15 available for testing
///
/// Each test uses unique FDW/server/table/key-prefix names to avoid
/// cross-test contamination, and TRUNCATEs the table before seeding so
/// rerunning against a dirty Redis still passes.
#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    const REDIS_HOST_PORT: &str = "127.0.0.1:8899";
    const TEST_DATABASE: &str = "15";

    fn setup(fdw: &str, server: &str) {
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {fdw} CASCADE;"
        ));
        Spi::run(&format!(
            "CREATE FOREIGN DATA WRAPPER {fdw} HANDLER redis_fdw_handler;"
        ))
        .unwrap();
        Spi::run(&format!(
            "CREATE SERVER {server} FOREIGN DATA WRAPPER {fdw} \
             OPTIONS (host_port '{REDIS_HOST_PORT}');"
        ))
        .unwrap();
    }

    fn teardown(fdw: &str) {
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {fdw} CASCADE;"
        ));
    }

    fn create_table(table: &str, columns: &str, server: &str, table_type: &str, prefix: &str) {
        Spi::run(&format!(
            "CREATE FOREIGN TABLE {table} ({columns}) SERVER {server} \
             OPTIONS (database '{TEST_DATABASE}', table_type '{table_type}', \
             table_key_prefix '{prefix}');"
        ))
        .unwrap();
    }

    fn assert_str(sql: &str, expected: &str) {
        let got = Spi::get_one::<String>(sql).unwrap().unwrap_or_default();
        assert_eq!(got, expected, "SQL mismatch: {sql}");
    }

    fn assert_count(sql: &str, expected: i64) {
        let got = Spi::get_one::<i64>(sql).unwrap().unwrap();
        assert_eq!(got, expected, "SQL count mismatch: {sql}");
    }

    // ============================================================
    // HASH: WHERE field=/IN/LIKE + ORDER BY + LIMIT
    // ============================================================
    #[pg_test]
    fn test_wlo_hash_where_limit_order() {
        let fdw = "wlo_hash_fdw";
        let server = "wlo_hash_srv";
        let table = "wlo_hash";
        setup(fdw, server);
        create_table(
            table,
            "field text, value text",
            server,
            "hash",
            "wlo:hash:t1",
        );
        Spi::run(&format!("TRUNCATE {table};")).unwrap();

        // Seed: user:01..user:10
        for i in 1..=10 {
            Spi::run(&format!(
                "INSERT INTO {table} VALUES ('user:{i:02}', 'name_{i:02}');"
            ))
            .unwrap();
        }

        // (1) WHERE field = 'x' — HGET pushdown, single row
        assert_str(
            &format!("SELECT value FROM {table} WHERE field = 'user:03';"),
            "name_03",
        );

        // (2) WHERE field IN (...) ORDER BY field LIMIT 2 — HMGET pushdown
        assert_str(
            &format!(
                "SELECT string_agg(value, ',' ORDER BY field) \
                 FROM (SELECT field, value FROM {table} \
                       WHERE field IN ('user:05','user:02','user:07') \
                       ORDER BY field LIMIT 2) sub;"
            ),
            "name_02,name_05",
        );

        // (3) ORDER BY field DESC LIMIT 3 over full scan
        assert_str(
            &format!(
                "SELECT string_agg(field, ',' ORDER BY field DESC) \
                 FROM (SELECT field FROM {table} ORDER BY field DESC LIMIT 3) sub;"
            ),
            "user:10,user:09,user:08",
        );

        // (4) WHERE field LIKE 'user:0%' + count + bounded subquery
        assert_count(
            &format!("SELECT COUNT(*) FROM {table} WHERE field LIKE 'user:0%';"),
            9,
        );
        assert_count(
            &format!(
                "SELECT COUNT(*) FROM \
                 (SELECT field FROM {table} WHERE field LIKE 'user:0%' \
                  ORDER BY field LIMIT 4) sub;"
            ),
            4,
        );

        Spi::run(&format!("DROP FOREIGN TABLE {table};")).unwrap();
        teardown(fdw);
    }

    // ============================================================
    // ZSET: score range, ORDER BY score, LIMIT/OFFSET
    // ============================================================
    #[pg_test]
    fn test_wlo_zset_where_limit_order() {
        let fdw = "wlo_zset_fdw";
        let server = "wlo_zset_srv";
        let table = "wlo_zset";
        setup(fdw, server);
        create_table(
            table,
            "member text, score numeric",
            server,
            "zset",
            "wlo:zset:t1",
        );
        Spi::run(&format!("TRUNCATE {table};")).unwrap();

        // Seed: m10/10, m20/20, m30/30, m40/40, m50/50
        for s in [10, 20, 30, 40, 50] {
            Spi::run(&format!("INSERT INTO {table} VALUES ('m{s}', {s});")).unwrap();
        }

        // (1) WHERE member = 'x' — ZSCORE pushdown
        assert_str(
            &format!("SELECT score::text FROM {table} WHERE member = 'm30';"),
            "30",
        );

        // (2) WHERE score BETWEEN 20 AND 40 ORDER BY score ASC — ZRANGEBYSCORE
        assert_str(
            &format!(
                "SELECT string_agg(member, ',' ORDER BY score ASC) \
                 FROM {table} WHERE score >= 20 AND score <= 40;"
            ),
            "m20,m30,m40",
        );

        // (3) ORDER BY score DESC LIMIT 3 (top-N)
        assert_str(
            &format!(
                "SELECT string_agg(member, ',' ORDER BY score DESC) \
                 FROM (SELECT member, score FROM {table} \
                       ORDER BY score DESC LIMIT 3) sub;"
            ),
            "m50,m40,m30",
        );

        // (4) WHERE score >= 20 ORDER BY score ASC LIMIT 2 OFFSET 1
        assert_str(
            &format!(
                "SELECT string_agg(member, ',' ORDER BY score ASC) \
                 FROM (SELECT member, score FROM {table} \
                       WHERE score >= 20 \
                       ORDER BY score ASC LIMIT 2 OFFSET 1) sub;"
            ),
            "m30,m40",
        );

        Spi::run(&format!("DROP FOREIGN TABLE {table};")).unwrap();
        teardown(fdw);
    }

    // ============================================================
    // STREAM: stream_id pushdown (>=, <=, BETWEEN) + ORDER BY
    // ============================================================
    #[pg_test]
    fn test_wlo_stream_where_limit_order() {
        let fdw = "wlo_stream_fdw";
        let server = "wlo_stream_srv";
        let table = "wlo_stream";
        setup(fdw, server);
        create_table(
            table,
            "stream_id text, user_id text, action text",
            server,
            "stream",
            "wlo:stream:t1",
        );
        Spi::run(&format!("TRUNCATE {table};")).unwrap();

        // Seed with explicit IDs so range pushdown is deterministic
        for (id, user, act) in [
            ("1-1", "alice", "login"),
            ("2-1", "bob", "view"),
            ("3-1", "alice", "edit"),
            ("4-1", "carol", "login"),
            ("5-1", "bob", "logout"),
        ] {
            Spi::run(&format!(
                "INSERT INTO {table} VALUES ('{id}', '{user}', '{act}');"
            ))
            .unwrap();
        }

        // (1) WHERE stream_id = exact (XRANGE id id)
        assert_str(
            &format!("SELECT user_id FROM {table} WHERE stream_id = '3-1';"),
            "alice",
        );

        // (2) WHERE stream_id BETWEEN '2-1' AND '4-1' ORDER BY stream_id
        assert_str(
            &format!(
                "SELECT string_agg(user_id, ',' ORDER BY stream_id ASC) \
                 FROM {table} WHERE stream_id BETWEEN '2-1' AND '4-1';"
            ),
            "bob,alice,carol",
        );

        // (3) WHERE stream_id >= '3-1' (bounded XRANGE)
        assert_count(
            &format!("SELECT COUNT(*) FROM {table} WHERE stream_id >= '3-1';"),
            3,
        );

        // (4) ORDER BY stream_id DESC LIMIT 2 (newest first)
        assert_str(
            &format!(
                "SELECT string_agg(stream_id, ',' ORDER BY stream_id DESC) \
                 FROM (SELECT stream_id FROM {table} \
                       ORDER BY stream_id DESC LIMIT 2) sub;"
            ),
            "5-1,4-1",
        );

        Spi::run(&format!("DROP FOREIGN TABLE {table};")).unwrap();
        teardown(fdw);
    }

    // ============================================================
    // LIST: LIMIT/OFFSET + ORDER BY element
    // ============================================================
    #[pg_test]
    fn test_wlo_list_limit_order() {
        let fdw = "wlo_list_fdw";
        let server = "wlo_list_srv";
        let table = "wlo_list";
        setup(fdw, server);
        create_table(table, "element text", server, "list", "wlo:list:t1");
        Spi::run(&format!("TRUNCATE {table};")).unwrap();

        // Seed insertion-ordered: a,b,c,d,e
        for v in ["a", "b", "c", "d", "e"] {
            Spi::run(&format!("INSERT INTO {table} VALUES ('{v}');")).unwrap();
        }

        // (1) LIMIT 3 — preserves insertion order
        assert_str(
            &format!(
                "SELECT string_agg(element, '') \
                 FROM (SELECT element FROM {table} LIMIT 3) sub;"
            ),
            "abc",
        );

        // (2) LIMIT 2 OFFSET 2 — middle slice
        assert_str(
            &format!(
                "SELECT string_agg(element, '') \
                 FROM (SELECT element FROM {table} OFFSET 2 LIMIT 2) sub;"
            ),
            "cd",
        );

        // (3) ORDER BY element DESC LIMIT 2
        assert_str(
            &format!(
                "SELECT string_agg(element, ',' ORDER BY element DESC) \
                 FROM (SELECT element FROM {table} ORDER BY element DESC LIMIT 2) sub;"
            ),
            "e,d",
        );

        // (4) COUNT after seed
        assert_count(&format!("SELECT COUNT(*) FROM {table};"), 5);

        Spi::run(&format!("DROP FOREIGN TABLE {table};")).unwrap();
        teardown(fdw);
    }

    // ============================================================
    // SET: ORDER BY member, WHERE member IN, LIMIT
    // ============================================================
    #[pg_test]
    fn test_wlo_set_where_limit_order() {
        let fdw = "wlo_set_fdw";
        let server = "wlo_set_srv";
        let table = "wlo_set";
        setup(fdw, server);
        create_table(table, "member text", server, "set", "wlo:set:t1");
        Spi::run(&format!("TRUNCATE {table};")).unwrap();

        // Seed: deliberately out of order to verify PG sort
        for v in ["delta", "alpha", "charlie", "bravo", "echo"] {
            Spi::run(&format!("INSERT INTO {table} VALUES ('{v}');")).unwrap();
        }

        // (1) ORDER BY member ASC LIMIT 3
        assert_str(
            &format!(
                "SELECT string_agg(member, ',' ORDER BY member ASC) \
                 FROM (SELECT member FROM {table} ORDER BY member ASC LIMIT 3) sub;"
            ),
            "alpha,bravo,charlie",
        );

        // (2) WHERE member IN (...) ORDER BY member
        assert_str(
            &format!(
                "SELECT string_agg(member, ',' ORDER BY member) \
                 FROM {table} WHERE member IN ('echo','alpha','bravo');"
            ),
            "alpha,bravo,echo",
        );

        // (3) COUNT after seed
        assert_count(&format!("SELECT COUNT(*) FROM {table};"), 5);

        // (4) ORDER BY member DESC LIMIT 2 OFFSET 1
        assert_str(
            &format!(
                "SELECT string_agg(member, ',' ORDER BY member DESC) \
                 FROM (SELECT member FROM {table} \
                       ORDER BY member DESC OFFSET 1 LIMIT 2) sub;"
            ),
            "delta,charlie",
        );

        Spi::run(&format!("DROP FOREIGN TABLE {table};")).unwrap();
        teardown(fdw);
    }
}
