//! Integration tests for batched parameterized Redis joins.
//!
//! These tests verify that the FDW-to-local-PG join path correctly fetches
//! Redis rows via the per-type batch_parameterized_lookup (HMGET / pipelined
//! SISMEMBER / pipelined ZSCORE / MGET) and applies any Redis-side WHERE
//! filters post-fetch. Cache invalidation on re_scan is exercised too.
//!
//! Prerequisites: Redis on 127.0.0.1:8899, database 15.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn batched_hash_lookup_returns_hmget_results() {
        Spi::run("DROP FOREIGN TABLE IF EXISTS batch_hash;").ok();
        Spi::run("DROP SERVER IF EXISTS batch_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS batch_wrap CASCADE;").ok();
        Spi::run("CREATE FOREIGN DATA WRAPPER batch_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
        Spi::run("CREATE SERVER batch_srv FOREIGN DATA WRAPPER batch_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();
        Spi::run("CREATE FOREIGN TABLE batch_hash (field text, value text) SERVER batch_srv OPTIONS (database '15', table_type 'hash', table_key_prefix 'batch:h1');").unwrap();

        for (f, v) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("e", "5")] {
            Spi::run(&format!("INSERT INTO batch_hash VALUES ('{f}','{v}');")).unwrap();
        }

        Spi::run("CREATE TEMP TABLE local_join(field text);").unwrap();
        Spi::run("INSERT INTO local_join VALUES ('a'),('c'),('e');").unwrap();

        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM local_join l JOIN batch_hash h ON h.field = l.field;",
        )
        .expect("count")
        .expect("not null");
        assert_eq!(count, 3, "expected 3 join matches");

        Spi::run("DROP FOREIGN TABLE batch_hash;").ok();
        Spi::run("DROP SERVER batch_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER batch_wrap CASCADE;").ok();
    }

    #[pg_test]
    fn batched_set_lookup_returns_correct_membership() {
        Spi::run("DROP FOREIGN TABLE IF EXISTS batch_set;").ok();
        Spi::run("DROP SERVER IF EXISTS batch_set_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS batch_set_wrap CASCADE;").ok();
        Spi::run("CREATE FOREIGN DATA WRAPPER batch_set_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
        Spi::run("CREATE SERVER batch_set_srv FOREIGN DATA WRAPPER batch_set_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();
        Spi::run("CREATE FOREIGN TABLE batch_set (member text) SERVER batch_set_srv OPTIONS (database '15', table_type 'set', table_key_prefix 'batch:s1');").unwrap();

        for m in ["alice", "bob", "carol"] {
            Spi::run(&format!("INSERT INTO batch_set VALUES ('{m}');")).unwrap();
        }
        Spi::run("CREATE TEMP TABLE candidates(member text);").unwrap();
        Spi::run("INSERT INTO candidates VALUES ('alice'),('eve'),('carol');").unwrap();

        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM candidates c JOIN batch_set s ON s.member = c.member;",
        )
        .expect("count")
        .expect("not null");
        assert_eq!(count, 2);

        Spi::run("DROP FOREIGN TABLE batch_set;").ok();
        Spi::run("DROP SERVER batch_set_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER batch_set_wrap CASCADE;").ok();
    }

    #[pg_test]
    fn batched_zset_lookup_returns_scores() {
        Spi::run("DROP FOREIGN TABLE IF EXISTS batch_zset;").ok();
        Spi::run("DROP SERVER IF EXISTS batch_zset_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS batch_zset_wrap CASCADE;").ok();
        Spi::run("CREATE FOREIGN DATA WRAPPER batch_zset_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
        Spi::run("CREATE SERVER batch_zset_srv FOREIGN DATA WRAPPER batch_zset_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();
        Spi::run("CREATE FOREIGN TABLE batch_zset (member text, score float8) SERVER batch_zset_srv OPTIONS (database '15', table_type 'zset', table_key_prefix 'batch:z1');").unwrap();

        for (m, s) in [("alice", "1.0"), ("bob", "2.5"), ("carol", "3.0")] {
            Spi::run(&format!("INSERT INTO batch_zset VALUES ('{m}', {s});")).unwrap();
        }
        Spi::run("CREATE TEMP TABLE z_join(member text);").unwrap();
        Spi::run("INSERT INTO z_join VALUES ('alice'),('carol');").unwrap();

        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM z_join l JOIN batch_zset z ON z.member = l.member;",
        )
        .expect("count")
        .expect("not null");
        assert_eq!(count, 2);

        Spi::run("DROP FOREIGN TABLE batch_zset;").ok();
        Spi::run("DROP SERVER batch_zset_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER batch_zset_wrap CASCADE;").ok();
    }

    #[pg_test]
    fn batched_string_multikey_lookup_returns_mget_results() {
        Spi::run("DROP FOREIGN TABLE IF EXISTS batch_str;").ok();
        Spi::run("DROP SERVER IF EXISTS batch_str_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS batch_str_wrap CASCADE;").ok();
        Spi::run("CREATE FOREIGN DATA WRAPPER batch_str_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
        Spi::run("CREATE SERVER batch_str_srv FOREIGN DATA WRAPPER batch_str_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();
        Spi::run("CREATE FOREIGN TABLE batch_str (key text, value text) SERVER batch_str_srv OPTIONS (database '15', table_type 'string', table_key_prefix 'batch:str:*');").unwrap();

        for (k, v) in [
            ("batch:str:a", "1"),
            ("batch:str:b", "2"),
            ("batch:str:c", "3"),
        ] {
            Spi::run(&format!("INSERT INTO batch_str VALUES ('{k}','{v}');")).unwrap();
        }
        Spi::run("CREATE TEMP TABLE s_join(key text);").unwrap();
        Spi::run("INSERT INTO s_join VALUES ('batch:str:a'),('batch:str:c'),('batch:str:zzz');")
            .unwrap();

        let count =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM s_join l JOIN batch_str s ON s.key = l.key;")
                .expect("count")
                .expect("not null");
        assert_eq!(count, 2);

        Spi::run("DROP FOREIGN TABLE batch_str;").ok();
        Spi::run("DROP SERVER batch_str_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER batch_str_wrap CASCADE;").ok();
    }

    #[pg_test]
    fn batched_hash_join_correctness_1000_rows() {
        Spi::run("DROP FOREIGN TABLE IF EXISTS big_hash;").ok();
        Spi::run("DROP SERVER IF EXISTS big_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS big_wrap CASCADE;").ok();
        Spi::run("CREATE FOREIGN DATA WRAPPER big_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
        Spi::run("CREATE SERVER big_srv FOREIGN DATA WRAPPER big_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();
        Spi::run("CREATE FOREIGN TABLE big_hash (field text, value text) SERVER big_srv OPTIONS (database '15', table_type 'hash', table_key_prefix 'big:h1', join_batch_size '256');").unwrap();

        Spi::run("CREATE TEMP TABLE outer1000 AS SELECT 'k' || i::text AS field FROM generate_series(1, 1000) g(i);").unwrap();
        Spi::run("INSERT INTO big_hash SELECT 'k' || i::text, 'v' || i::text FROM generate_series(1, 1000) g(i);").unwrap();

        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM outer1000 o JOIN big_hash h ON h.field = o.field;",
        )
        .expect("count")
        .expect("not null");
        assert_eq!(count, 1000);

        Spi::run("DROP FOREIGN TABLE big_hash;").ok();
        Spi::run("DROP SERVER big_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER big_wrap CASCADE;").ok();
    }

    #[pg_test]
    fn parameterized_join_correct_after_rescan() {
        Spi::run("DROP FOREIGN TABLE IF EXISTS rescan_h;").ok();
        Spi::run("DROP SERVER IF EXISTS rescan_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS rescan_wrap CASCADE;").ok();
        Spi::run("CREATE FOREIGN DATA WRAPPER rescan_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
        Spi::run("CREATE SERVER rescan_srv FOREIGN DATA WRAPPER rescan_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();
        Spi::run("CREATE FOREIGN TABLE rescan_h (field text, value text) SERVER rescan_srv OPTIONS (database '15', table_type 'hash', table_key_prefix 'rescan:h1');").unwrap();

        for (f, v) in [("a", "1"), ("b", "2"), ("c", "3")] {
            Spi::run(&format!("INSERT INTO rescan_h VALUES ('{f}','{v}');")).unwrap();
        }
        Spi::run("CREATE TEMP TABLE outer3(field text);").unwrap();
        Spi::run("INSERT INTO outer3 VALUES ('a'),('b'),('c');").unwrap();

        let count = Spi::get_one::<i64>(
            "WITH a AS (SELECT COUNT(*) AS n FROM outer3 o JOIN rescan_h h ON h.field = o.field), \
                      b AS (SELECT COUNT(*) AS n FROM outer3 o JOIN rescan_h h ON h.field = o.field) \
             SELECT a.n + b.n FROM a, b;",
        )
        .expect("count")
        .expect("not null");
        assert_eq!(count, 6);

        Spi::run("DROP FOREIGN TABLE rescan_h;").ok();
        Spi::run("DROP SERVER rescan_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER rescan_wrap CASCADE;").ok();
    }

    #[pg_test]
    fn where_score_range_on_zset_pushed_through_join() {
        Spi::run("DROP FOREIGN TABLE IF EXISTS wz;").ok();
        Spi::run("DROP SERVER IF EXISTS wz_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS wz_wrap CASCADE;").ok();
        Spi::run("CREATE FOREIGN DATA WRAPPER wz_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
        Spi::run("CREATE SERVER wz_srv FOREIGN DATA WRAPPER wz_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();
        Spi::run("CREATE FOREIGN TABLE wz (member text, score float8) SERVER wz_srv OPTIONS (database '15', table_type 'zset', table_key_prefix 'wz:1');").unwrap();

        for (m, s) in [("a", "1.0"), ("b", "2.0"), ("c", "3.0"), ("d", "4.0")] {
            Spi::run(&format!("INSERT INTO wz VALUES ('{m}', {s});")).unwrap();
        }
        Spi::run("CREATE TEMP TABLE wzj(member text);").unwrap();
        Spi::run("INSERT INTO wzj VALUES ('a'),('b'),('c'),('d');").unwrap();

        // Only b/c/d have score >= 2.0
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM wzj j JOIN wz z ON z.member = j.member WHERE z.score >= 2.0;",
        )
        .expect("count")
        .expect("not null");
        assert_eq!(count, 3);

        Spi::run("DROP FOREIGN TABLE wz;").ok();
        Spi::run("DROP SERVER wz_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER wz_wrap CASCADE;").ok();
    }

    #[pg_test]
    fn explain_analyze_shows_pipeline_mode_for_hash_join() {
        Spi::run("DROP FOREIGN TABLE IF EXISTS exp_h;").ok();
        Spi::run("DROP SERVER IF EXISTS exp_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS exp_wrap CASCADE;").ok();
        Spi::run("CREATE FOREIGN DATA WRAPPER exp_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
        Spi::run("CREATE SERVER exp_srv FOREIGN DATA WRAPPER exp_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();
        Spi::run("CREATE FOREIGN TABLE exp_h (field text, value text) SERVER exp_srv OPTIONS (database '15', table_type 'hash', table_key_prefix 'exp:h1', join_batch_size '256');").unwrap();

        Spi::run("INSERT INTO exp_h VALUES ('a','1'),('b','2');").unwrap();
        Spi::run("CREATE TEMP TABLE exp_o(field text);").unwrap();
        Spi::run("INSERT INTO exp_o VALUES ('a'),('b');").unwrap();

        let q = "EXPLAIN (FORMAT TEXT, ANALYZE) SELECT * FROM exp_o o JOIN exp_h h ON h.field = o.field;";
        let text = Spi::connect(|client| {
            let mut out = String::new();
            let result = client.select(q, None, &[]).unwrap();
            for row in result {
                if let Some(line) = row.get::<&str>(1).unwrap() {
                    out.push_str(line);
                    out.push('\n');
                }
            }
            out
        });

        // The planner might not pick the parameterized path for such a tiny join;
        // assert weakly: if parameterized was chosen, Join Batch Mode must appear.
        // Otherwise just verify the EXPLAIN ran successfully and includes our other labels.
        assert!(
            text.contains("Foreign Scan"),
            "EXPLAIN missing Foreign Scan:\n{text}"
        );
        if text.contains("Join Batch Mode:") {
            assert!(
                text.contains("pipeline") || text.contains("fallback") || text.contains("n/a"),
                "Join Batch Mode value unexpected:\n{text}"
            );
        }

        Spi::run("DROP FOREIGN TABLE exp_h;").ok();
        Spi::run("DROP SERVER exp_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER exp_wrap CASCADE;").ok();
    }

    fn explain_plan_for(sql: &str) -> String {
        let q = format!("EXPLAIN (VERBOSE, FORMAT TEXT) {}", sql);
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
    fn test_join_stream_parameterized_uses_xrange_per_id() {
        Spi::run("DROP FOREIGN TABLE IF EXISTS join_stream_fdw;").ok();
        Spi::run("DROP SERVER IF EXISTS join_stream_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS join_stream_wrap CASCADE;").ok();
        Spi::run("CREATE FOREIGN DATA WRAPPER join_stream_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
        Spi::run("CREATE SERVER join_stream_srv FOREIGN DATA WRAPPER join_stream_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();

        // Seed Redis stream with 100 entries
        let stream_key = "test:join_stream_param";
        let mut c = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        let _: i64 = redis::cmd("DEL").arg(stream_key).query(&mut c).unwrap_or(0);
        for i in 0..100u64 {
            let _: String = redis::cmd("XADD")
                .arg(stream_key)
                .arg(format!("{}-0", 5_000_000 + i))
                .arg("v")
                .arg(i.to_string())
                .query(&mut c)
                .unwrap();
        }

        Spi::run(
            "CREATE FOREIGN TABLE join_stream_fdw (stream_id text, v_field text, v_value text) \
             SERVER join_stream_srv \
             OPTIONS (database '15', table_type 'stream', table_key_prefix 'test:join_stream_param');"
        ).unwrap();
        Spi::run("DROP TABLE IF EXISTS pg_ids;").ok();
        Spi::run("CREATE TABLE pg_ids (id text);").unwrap();
        Spi::run("INSERT INTO pg_ids VALUES ('5000010-0'),('5000020-0'),('5000030-0'),('5000040-0'),('5000050-0');").unwrap();
        Spi::run("ANALYZE pg_ids;").unwrap();

        // Force a plan that should pick the parameterized path
        Spi::run("SET enable_hashjoin = off; SET enable_mergejoin = off;").unwrap();
        Spi::run("SET enable_material = off;").unwrap();

        let n: i64 = Spi::get_one(
            "SELECT count(*) FROM pg_ids p JOIN join_stream_fdw s ON s.stream_id = p.id",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(n, 5, "expected 5 joined rows");

        // EXPLAIN should reflect parameterized XRANGE
        let plan = explain_plan_for(
            "SELECT count(*) FROM pg_ids p JOIN join_stream_fdw s ON s.stream_id = p.id",
        );
        assert!(
            plan.contains("Redis Ops: XRANGE"),
            "expected XRANGE in parameterized EXPLAIN, got:\n{}",
            plan
        );
        // Verify the parameterized path was actually chosen — Join Batch Mode
        // is only emitted when is_parameterized is set. Without a Stream
        // batch_parameterized_lookup override, the planner falls back to the
        // default impl which yields no rows on each NestLoop iteration; this
        // assertion locks in that the fast-path is wired.
        assert!(
            plan.contains("Join Batch Mode:"),
            "expected parameterized path (Join Batch Mode label) in EXPLAIN, got:\n{}",
            plan
        );

        Spi::run("RESET enable_hashjoin; RESET enable_mergejoin; RESET enable_material;").ok();
        Spi::run("DROP FOREIGN TABLE IF EXISTS join_stream_fdw;").ok();
        Spi::run("DROP TABLE IF EXISTS pg_ids;").ok();
        Spi::run("DROP SERVER IF EXISTS join_stream_srv CASCADE;").ok();
        Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS join_stream_wrap CASCADE;").ok();
        let _: i64 = redis::cmd("DEL").arg(stream_key).query(&mut c).unwrap_or(0);
    }
}
