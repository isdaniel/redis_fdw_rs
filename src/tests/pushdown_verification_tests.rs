/// Pushdown verification tests — assert that optimized Redis commands (HGET, SISMEMBER, ZSCORE)
/// are actually used instead of cursor-based SCAN when WHERE conditions support direct lookup.
///
/// Uses before/after delta on Redis INFO COMMANDSTATS to count commands issued during a query.
#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;
    use std::collections::HashMap;

    const REDIS_URL: &str = "redis://127.0.0.1:8899/";
    const DATA_SIZE: usize = 50;

    fn redis_conn() -> redis::Connection {
        redis::Client::open(REDIS_URL)
            .expect("Failed to create Redis client")
            .get_connection()
            .expect("Failed to connect to Redis")
    }

    fn get_all_command_counts() -> HashMap<String, u64> {
        let mut conn = redis_conn();
        let info: String = redis::cmd("INFO")
            .arg("commandstats")
            .query(&mut conn)
            .expect("Failed to get commandstats");

        let mut counts = HashMap::new();
        for line in info.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("cmdstat_") {
                // Format: "hget:calls=3,usec=12,..." or "config|resetstat:calls=1,..."
                if let Some(colon_pos) = rest.find(':') {
                    let cmd_name = &rest[..colon_pos];
                    let stats_part = &rest[colon_pos + 1..];
                    if let Some(calls_val) = stats_part.split(',').next() {
                        if let Some(num_str) = calls_val.strip_prefix("calls=") {
                            if let Ok(num) = num_str.parse::<u64>() {
                                counts.insert(cmd_name.to_lowercase(), num);
                            }
                        }
                    }
                }
            }
        }
        counts
    }

    fn command_delta(
        before: &HashMap<String, u64>,
        after: &HashMap<String, u64>,
        cmd: &str,
    ) -> u64 {
        let cmd = cmd.to_lowercase();
        let b = before.get(&cmd).copied().unwrap_or(0);
        let a = after.get(&cmd).copied().unwrap_or(0);
        a.saturating_sub(b)
    }

    fn get_one(query: &str) -> Option<String> {
        Spi::get_one::<String>(query).unwrap()
    }

    fn get_count(query: &str) -> i64 {
        Spi::get_one::<i64>(query).unwrap().unwrap_or(0)
    }

    fn setup_fdw(table_name: &str, columns: &str, table_type: &str, key_prefix: &str) {
        let wrapper = format!("pv_{}_wrapper", table_name);
        let server = format!("pv_{}_server", table_name);

        Spi::run(&format!(
            "CREATE FOREIGN DATA WRAPPER {wrapper} HANDLER redis_fdw_handler;"
        ))
        .unwrap();
        Spi::run(&format!(
            "CREATE SERVER {server} FOREIGN DATA WRAPPER {wrapper} \
             OPTIONS (host_port '127.0.0.1:8899');"
        ))
        .unwrap();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE {table_name} ({columns}) \
             SERVER {server} OPTIONS (\
               database '15', \
               table_type '{table_type}', \
               table_key_prefix '{key_prefix}'\
             );"
        ))
        .unwrap();
    }

    fn teardown_fdw(table_name: &str) {
        let wrapper = format!("pv_{}_wrapper", table_name);
        Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {table_name};")).unwrap();
        Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {wrapper} CASCADE;"
        ))
        .unwrap();
    }

    fn cleanup_redis_key(key: &str) {
        let mut conn = redis_conn();
        let _: () = redis::cmd("DEL").arg(key).query(&mut conn).unwrap();
    }

    // ── Hash tests ──────────────────────────────────────────────────────

    #[pg_test]
    fn test_pushdown_verify_hash_equal_uses_hget() {
        let table = "pv_hash_eq";
        let key = "pv_test:hash_eq";
        cleanup_redis_key(key);
        setup_fdw(table, "field text, value text", "hash", key);

        for i in 0..DATA_SIZE {
            Spi::run(&format!("INSERT INTO {table} VALUES ('f{i}', 'v{i}');")).unwrap();
        }

        let before = get_all_command_counts();
        let result = get_one(&format!("SELECT value FROM {table} WHERE field = 'f25'"));
        let after = get_all_command_counts();

        assert_eq!(result.as_deref(), Some("v25"));

        let hget_delta = command_delta(&before, &after, "hget");
        assert!(
            hget_delta >= 1,
            "Expected HGET for = pushdown, but delta={}",
            hget_delta
        );

        teardown_fdw(table);
        cleanup_redis_key(key);
    }

    #[pg_test]
    fn test_pushdown_verify_hash_in_uses_hmget() {
        let table = "pv_hash_in";
        let key = "pv_test:hash_in";
        cleanup_redis_key(key);
        setup_fdw(table, "field text, value text", "hash", key);

        for i in 0..DATA_SIZE {
            Spi::run(&format!("INSERT INTO {table} VALUES ('f{i}', 'v{i}');")).unwrap();
        }

        let before = get_all_command_counts();
        let count = get_count(&format!(
            "SELECT COUNT(*) FROM {table} WHERE field IN ('f1', 'f2', 'f3')"
        ));
        let after = get_all_command_counts();

        assert_eq!(count, 3, "IN query should return 3 rows");

        let hmget_delta = command_delta(&before, &after, "hmget");
        assert!(
            hmget_delta >= 1,
            "Expected HMGET for IN pushdown, but delta={}",
            hmget_delta
        );

        teardown_fdw(table);
        cleanup_redis_key(key);
    }

    #[pg_test]
    fn test_pushdown_verify_hash_no_condition_uses_hscan() {
        let table = "pv_hash_scan";
        let key = "pv_test:hash_scan";
        cleanup_redis_key(key);
        setup_fdw(table, "field text, value text", "hash", key);

        for i in 0..DATA_SIZE {
            Spi::run(&format!("INSERT INTO {table} VALUES ('f{i}', 'v{i}');")).unwrap();
        }

        let before = get_all_command_counts();
        let count = get_count(&format!("SELECT COUNT(*) FROM {table}"));
        let after = get_all_command_counts();

        assert_eq!(count, DATA_SIZE as i64, "Full scan should return all rows");

        let hscan_delta = command_delta(&before, &after, "hscan");
        assert!(
            hscan_delta >= 1,
            "Expected HSCAN for full scan (no WHERE), but delta={}",
            hscan_delta
        );

        teardown_fdw(table);
        cleanup_redis_key(key);
    }

    // ── Set tests ───────────────────────────────────────────────────────

    #[pg_test]
    fn test_pushdown_verify_set_equal_uses_sismember() {
        let table = "pv_set_eq";
        let key = "pv_test:set_eq";
        cleanup_redis_key(key);
        setup_fdw(table, "member text", "set", key);

        for i in 0..DATA_SIZE {
            Spi::run(&format!("INSERT INTO {table} VALUES ('m{i}');")).unwrap();
        }

        let before = get_all_command_counts();
        let result = get_one(&format!("SELECT member FROM {table} WHERE member = 'm25'"));
        let after = get_all_command_counts();

        assert_eq!(result.as_deref(), Some("m25"));

        let sismember_delta = command_delta(&before, &after, "sismember");
        assert!(
            sismember_delta >= 1,
            "Expected SISMEMBER for = pushdown, but delta={}",
            sismember_delta
        );

        teardown_fdw(table);
        cleanup_redis_key(key);
    }

    #[pg_test]
    fn test_pushdown_verify_set_in_uses_smismember() {
        let table = "pv_set_in";
        let key = "pv_test:set_in";
        cleanup_redis_key(key);
        setup_fdw(table, "member text", "set", key);

        for i in 0..DATA_SIZE {
            Spi::run(&format!("INSERT INTO {table} VALUES ('m{i}');")).unwrap();
        }

        let before = get_all_command_counts();
        let count = get_count(&format!(
            "SELECT COUNT(*) FROM {table} WHERE member IN ('m1', 'm2', 'm3')"
        ));
        let after = get_all_command_counts();

        assert_eq!(count, 3, "IN query should return 3 matching members");

        let smismember_delta = command_delta(&before, &after, "smismember");
        let sismember_delta = command_delta(&before, &after, "sismember");
        assert!(
            smismember_delta >= 1 || sismember_delta >= 1,
            "Expected SMISMEMBER or SISMEMBER for IN pushdown, \
             but smismember_delta={}, sismember_delta={}",
            smismember_delta,
            sismember_delta
        );

        teardown_fdw(table);
        cleanup_redis_key(key);
    }

    // ── ZSet tests ──────────────────────────────────────────────────────

    #[pg_test]
    fn test_pushdown_verify_zset_equal_uses_zscore() {
        let table = "pv_zset_eq";
        let key = "pv_test:zset_eq";
        cleanup_redis_key(key);
        setup_fdw(table, "member text, score text", "zset", key);

        for i in 0..DATA_SIZE {
            Spi::run(&format!("INSERT INTO {table} VALUES ('m{i}', '{}.0');", i)).unwrap();
        }

        let before = get_all_command_counts();
        let result = get_one(&format!("SELECT score FROM {table} WHERE member = 'm25'"));
        let after = get_all_command_counts();

        assert!(result.is_some(), "Should find m25");

        let zscore_delta = command_delta(&before, &after, "zscore");
        assert!(
            zscore_delta >= 1,
            "Expected ZSCORE for = pushdown, but delta={}",
            zscore_delta
        );

        teardown_fdw(table);
        cleanup_redis_key(key);
    }

    #[pg_test]
    fn test_pushdown_verify_zset_in_uses_zscore() {
        let table = "pv_zset_in";
        let key = "pv_test:zset_in";
        cleanup_redis_key(key);
        setup_fdw(table, "member text, score text", "zset", key);

        for i in 0..DATA_SIZE {
            Spi::run(&format!("INSERT INTO {table} VALUES ('m{i}', '{}.0');", i)).unwrap();
        }

        let before = get_all_command_counts();
        let count = get_count(&format!(
            "SELECT COUNT(*) FROM {table} WHERE member IN ('m1', 'm2')"
        ));
        let after = get_all_command_counts();

        assert_eq!(count, 2, "IN query should return 2 matching members");

        let zmscore_delta = command_delta(&before, &after, "zmscore");
        let zscore_delta = command_delta(&before, &after, "zscore");
        assert!(
            zmscore_delta >= 1 || zscore_delta >= 1,
            "Expected ZMSCORE or ZSCORE for IN pushdown, \
             but zmscore_delta={}, zscore_delta={}",
            zmscore_delta,
            zscore_delta
        );

        teardown_fdw(table);
        cleanup_redis_key(key);
    }

    // ── String tests ────────────────────────────────────────────────────

    #[pg_test]
    fn test_pushdown_verify_string_uses_get() {
        let table = "pv_string_eq";
        let key = "pv_test:string_eq";
        cleanup_redis_key(key);
        setup_fdw(table, "value text", "string", key);

        Spi::run(&format!("INSERT INTO {table} VALUES ('target_value');")).unwrap();

        let before = get_all_command_counts();
        let result = get_one(&format!(
            "SELECT value FROM {table} WHERE value = 'target_value'"
        ));
        let after = get_all_command_counts();

        assert_eq!(result.as_deref(), Some("target_value"));

        let get_delta = command_delta(&before, &after, "get");
        assert!(
            get_delta >= 1,
            "Expected GET for string lookup, but delta={}",
            get_delta
        );

        teardown_fdw(table);
        cleanup_redis_key(key);
    }

    #[pg_test]
    fn test_pushdown_verify_zset_score_gte_uses_zrangebyscore() {
        let table = "pv_zset_score_gte";
        let key = "pv_test:zset_score_gte";
        cleanup_redis_key(key);
        setup_fdw(table, "member text, score numeric", "zset", key);

        for i in 0..100 {
            Spi::run(&format!("INSERT INTO {table} VALUES ('m{i}', {i});")).unwrap();
        }

        let before = get_all_command_counts();
        let count = get_count(&format!("SELECT COUNT(*) FROM {table} WHERE score >= 90"));
        let after = get_all_command_counts();

        assert_eq!(count, 10, "score >= 90 should return 10 members (90..99)");

        let zrangebyscore_delta = command_delta(&before, &after, "zrangebyscore");
        assert!(
            zrangebyscore_delta >= 1,
            "Expected ZRANGEBYSCORE for >= pushdown, but delta={}",
            zrangebyscore_delta
        );

        teardown_fdw(table);
        cleanup_redis_key(key);
    }

    #[pg_test]
    fn test_pushdown_verify_zset_score_range_uses_zrangebyscore() {
        let table = "pv_zset_score_range";
        let key = "pv_test:zset_score_range";
        cleanup_redis_key(key);
        setup_fdw(table, "member text, score numeric", "zset", key);

        for i in 0..100 {
            Spi::run(&format!("INSERT INTO {table} VALUES ('m{i}', {i});")).unwrap();
        }

        let before = get_all_command_counts();
        let count = get_count(&format!(
            "SELECT COUNT(*) FROM {table} WHERE score >= 20 AND score <= 30"
        ));
        let after = get_all_command_counts();

        assert_eq!(count, 11, "20 <= score <= 30 should return 11 members");

        let zrangebyscore_delta = command_delta(&before, &after, "zrangebyscore");
        assert!(
            zrangebyscore_delta >= 1,
            "Expected ZRANGEBYSCORE for range pushdown, but delta={}",
            zrangebyscore_delta
        );

        teardown_fdw(table);
        cleanup_redis_key(key);
    }

    #[pg_test]
    fn test_pushdown_verify_zset_score_gt_exclusive() {
        let table = "pv_zset_score_gt";
        let key = "pv_test:zset_score_gt";
        cleanup_redis_key(key);
        setup_fdw(table, "member text, score numeric", "zset", key);

        for i in 0..50 {
            Spi::run(&format!("INSERT INTO {table} VALUES ('m{i}', {i});")).unwrap();
        }

        let count = get_count(&format!("SELECT COUNT(*) FROM {table} WHERE score > 45"));
        assert_eq!(count, 4, "score > 45 should return 4 members (46,47,48,49)");

        teardown_fdw(table);
        cleanup_redis_key(key);
    }

    #[pg_test]
    fn test_pushdown_verify_zset_score_lt_uses_zrangebyscore() {
        let table = "pv_zset_score_lt";
        let key = "pv_test:zset_score_lt";
        cleanup_redis_key(key);
        setup_fdw(table, "member text, score numeric", "zset", key);

        for i in 0..100 {
            Spi::run(&format!("INSERT INTO {table} VALUES ('m{i}', {i});")).unwrap();
        }

        let before = get_all_command_counts();
        let count = get_count(&format!("SELECT COUNT(*) FROM {table} WHERE score < 5"));
        let after = get_all_command_counts();

        assert_eq!(count, 5, "score < 5 should return 5 members (0,1,2,3,4)");

        let zrangebyscore_delta = command_delta(&before, &after, "zrangebyscore");
        assert!(
            zrangebyscore_delta >= 1,
            "Expected ZRANGEBYSCORE for < pushdown, but delta={}",
            zrangebyscore_delta
        );

        teardown_fdw(table);
        cleanup_redis_key(key);
    }

    #[pg_test]
    fn test_pushdown_verify_zset_score_range_with_ttl_first() {
        let table = "pv_zset_score_ttl";
        let key = "pv_test:zset_score_ttl";
        cleanup_redis_key(key);

        let wrapper = format!("pv_{}_wrapper", table);
        let server = format!("pv_{}_server", table);
        Spi::run(&format!(
            "CREATE FOREIGN DATA WRAPPER {wrapper} HANDLER redis_fdw_handler;"
        ))
        .unwrap();
        Spi::run(&format!(
            "CREATE SERVER {server} FOREIGN DATA WRAPPER {wrapper} \
             OPTIONS (host_port '127.0.0.1:8899');"
        ))
        .unwrap();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE {table} (ttl bigint, member text, score numeric) \
             SERVER {server} OPTIONS (\
               database '15', \
               table_type 'zset', \
               table_key_prefix '{key}', \
               ttl '600'\
             );"
        ))
        .unwrap();

        for i in 0..50 {
            Spi::run(&format!(
                "INSERT INTO {table} (member, score) VALUES ('player{i}', {});",
                i * 10
            ))
            .unwrap();
        }

        let count = get_count(&format!("SELECT COUNT(*) FROM {table} WHERE score >= 400"));
        assert_eq!(
            count, 10,
            "score >= 400 with TTL-first should return 10 members (40..49)"
        );

        let before = get_all_command_counts();
        let _ = get_count(&format!("SELECT COUNT(*) FROM {table} WHERE score >= 400"));
        let after = get_all_command_counts();

        let zrangebyscore_delta = command_delta(&before, &after, "zrangebyscore");
        assert!(
            zrangebyscore_delta >= 1,
            "Expected ZRANGEBYSCORE even with TTL at position 0, but delta={}",
            zrangebyscore_delta
        );

        teardown_fdw(table);
        cleanup_redis_key(key);
    }
}
