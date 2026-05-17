/// Integration tests for commit 3f04fdeb changes:
/// - Memory-safe state management (leak_and_drop_on_delete)
/// - Streaming iteration (fetch_next_batch / scan_complete)
/// - Modified serialization path (serialize_ptr_to_list / deserialize_ptr_from_list)
/// - Rescan with streaming state reset
/// - Full CRUD coverage for all Redis types under the new handlers
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
    const FDW_NAME: &str = "streaming_modify_test_fdw";
    const SERVER_NAME: &str = "streaming_modify_test_srv";

    fn setup() {
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {FDW_NAME} CASCADE;"
        ));
        Spi::run(&format!(
            "CREATE FOREIGN DATA WRAPPER {FDW_NAME} HANDLER redis_fdw_handler;"
        ))
        .unwrap();
        Spi::run(&format!(
            "CREATE SERVER {SERVER_NAME} FOREIGN DATA WRAPPER {FDW_NAME} \
             OPTIONS (host_port '{REDIS_HOST_PORT}');"
        ))
        .unwrap();
    }

    fn teardown() {
        let _ = Spi::run(&format!("DROP SERVER IF EXISTS {SERVER_NAME} CASCADE;"));
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {FDW_NAME} CASCADE;"
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

    fn get_one(sql: &str) -> String {
        Spi::get_one::<String>(sql).unwrap().unwrap()
    }

    // ═══════════════════════════════════════════════════════════════════════
    // STRING: SELECT, INSERT, UPDATE, DELETE
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    fn test_streaming_string_full_crud() {
        setup();
        let t = "sm_string";
        let k = "sm:string:crud";
        create_table(t, "value TEXT", "string", k);

        // INSERT
        Spi::run(&format!("INSERT INTO {t} (value) VALUES ('initial');")).unwrap();
        assert_eq!(count(t), 1);
        assert_eq!(get_one(&format!("SELECT value FROM {t};")), "initial");

        // UPDATE (exercises exec_foreign_update with new ptr serialization)
        Spi::run(&format!("UPDATE {t} SET value = 'modified';")).unwrap();
        assert_eq!(count(t), 1);
        assert_eq!(get_one(&format!("SELECT value FROM {t};")), "modified");

        // UPDATE again (ensures memory context survives multiple modify cycles)
        Spi::run(&format!("UPDATE {t} SET value = 'final';")).unwrap();
        assert_eq!(get_one(&format!("SELECT value FROM {t};")), "final");

        // DELETE
        Spi::run(&format!("DELETE FROM {t};")).unwrap();
        assert_eq!(count(t), 0);

        drop_table(t);
        teardown();
    }

    #[pg_test]
    fn test_streaming_string_insert_after_delete() {
        setup();
        let t = "sm_string_iad";
        let k = "sm:string:insertafterdel";
        create_table(t, "value TEXT", "string", k);

        Spi::run(&format!("INSERT INTO {t} (value) VALUES ('first');")).unwrap();
        Spi::run(&format!("DELETE FROM {t};")).unwrap();
        assert_eq!(count(t), 0);

        // Re-insert after delete (tests fresh scan after modify)
        Spi::run(&format!("INSERT INTO {t} (value) VALUES ('second');")).unwrap();
        assert_eq!(count(t), 1);
        assert_eq!(get_one(&format!("SELECT value FROM {t};")), "second");

        Spi::run(&format!("DELETE FROM {t};")).unwrap();
        drop_table(t);
        teardown();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // HASH: SELECT, INSERT, UPDATE, DELETE
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    fn test_streaming_hash_full_crud() {
        setup();
        let t = "sm_hash";
        let k = "sm:hash:crud";
        create_table(t, "field TEXT, value TEXT", "hash", k);

        // INSERT multiple fields
        Spi::run(&format!("INSERT INTO {t} VALUES ('name', 'Alice');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} VALUES ('age', '30');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} VALUES ('city', 'Tokyo');")).unwrap();
        assert_eq!(count(t), 3);

        // SELECT with pushdown
        assert_eq!(
            get_one(&format!("SELECT value FROM {t} WHERE field = 'name';")),
            "Alice"
        );

        // UPDATE value (new serialization path: plan_foreign_modify → begin_foreign_modify)
        Spi::run(&format!(
            "UPDATE {t} SET value = 'Bob' WHERE field = 'name';"
        ))
        .unwrap();
        assert_eq!(
            get_one(&format!("SELECT value FROM {t} WHERE field = 'name';")),
            "Bob"
        );

        // UPDATE field rename (HDEL old + HSET new)
        Spi::run(&format!(
            "UPDATE {t} SET field = 'location', value = 'Tokyo' WHERE field = 'city';"
        ))
        .unwrap();
        assert_eq!(count_where(t, "field = 'city'"), 0);
        assert_eq!(
            get_one(&format!("SELECT value FROM {t} WHERE field = 'location';")),
            "Tokyo"
        );

        // DELETE specific field
        Spi::run(&format!("DELETE FROM {t} WHERE field = 'age';")).unwrap();
        assert_eq!(count(t), 2);

        // DELETE remaining
        Spi::run(&format!("DELETE FROM {t} WHERE field = 'name';")).unwrap();
        Spi::run(&format!("DELETE FROM {t} WHERE field = 'location';")).unwrap();
        assert_eq!(count(t), 0);

        drop_table(t);
        teardown();
    }

    #[pg_test]
    fn test_streaming_hash_batch_select() {
        setup();
        let t = "sm_hash_batch";
        let k = "sm:hash:batch";
        create_table(t, "field TEXT, value TEXT", "hash", k);

        // Insert enough fields to exercise iteration logic
        for i in 0..20 {
            Spi::run(&format!(
                "INSERT INTO {t} VALUES ('field_{i}', 'value_{i}');"
            ))
            .unwrap();
        }
        assert_eq!(count(t), 20);

        // Verify pushdown on a specific field
        assert_eq!(
            get_one(&format!("SELECT value FROM {t} WHERE field = 'field_15';")),
            "value_15"
        );

        // Clean up all fields
        for i in 0..20 {
            Spi::run(&format!("DELETE FROM {t} WHERE field = 'field_{i}';")).unwrap();
        }
        assert_eq!(count(t), 0);

        drop_table(t);
        teardown();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // LIST: SELECT, INSERT, UPDATE, DELETE
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    fn test_streaming_list_full_crud() {
        setup();
        let t = "sm_list";
        let k = "sm:list:crud";
        create_table(t, "value TEXT", "list", k);

        // INSERT multiple items (RPUSH)
        Spi::run(&format!("INSERT INTO {t} (value) VALUES ('alpha');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} (value) VALUES ('beta');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} (value) VALUES ('gamma');")).unwrap();
        assert_eq!(count(t), 3);

        // SELECT with pushdown
        assert_eq!(count_where(t, "value = 'beta'"), 1);

        // UPDATE (LPOS + LSET)
        Spi::run(&format!(
            "UPDATE {t} SET value = 'delta' WHERE value = 'beta';"
        ))
        .unwrap();
        assert_eq!(count_where(t, "value = 'beta'"), 0);
        assert_eq!(count_where(t, "value = 'delta'"), 1);

        // DELETE specific item (LREM)
        Spi::run(&format!("DELETE FROM {t} WHERE value = 'alpha';")).unwrap();
        assert_eq!(count(t), 2);

        // DELETE remaining
        Spi::run(&format!("DELETE FROM {t} WHERE value = 'delta';")).unwrap();
        Spi::run(&format!("DELETE FROM {t} WHERE value = 'gamma';")).unwrap();
        assert_eq!(count(t), 0);

        drop_table(t);
        teardown();
    }

    #[pg_test]
    fn test_streaming_list_ordering_preserved() {
        setup();
        let t = "sm_list_order";
        let k = "sm:list:order";
        create_table(t, "value TEXT", "list", k);

        Spi::run(&format!("INSERT INTO {t} (value) VALUES ('first');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} (value) VALUES ('second');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} (value) VALUES ('third');")).unwrap();

        // List maintains insertion order; LIMIT 1 returns first element
        let first = get_one(&format!("SELECT value FROM {t} LIMIT 1;"));
        assert_eq!(first, "first");

        Spi::run(&format!("DELETE FROM {t} WHERE value = 'first';")).unwrap();
        Spi::run(&format!("DELETE FROM {t} WHERE value = 'second';")).unwrap();
        Spi::run(&format!("DELETE FROM {t} WHERE value = 'third';")).unwrap();

        drop_table(t);
        teardown();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SET: SELECT, INSERT, UPDATE, DELETE
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    fn test_streaming_set_full_crud() {
        setup();
        let t = "sm_set";
        let k = "sm:set:crud";
        create_table(t, "member TEXT", "set", k);

        // INSERT members (SADD)
        Spi::run(&format!("INSERT INTO {t} (member) VALUES ('apple');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} (member) VALUES ('banana');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} (member) VALUES ('cherry');")).unwrap();
        assert_eq!(count(t), 3);

        // SELECT with pushdown
        assert_eq!(count_where(t, "member = 'banana'"), 1);
        assert_eq!(count_where(t, "member = 'nonexist'"), 0);

        // UPDATE member (SREM old + SADD new)
        Spi::run(&format!(
            "UPDATE {t} SET member = 'date' WHERE member = 'banana';"
        ))
        .unwrap();
        assert_eq!(count_where(t, "member = 'banana'"), 0);
        assert_eq!(count_where(t, "member = 'date'"), 1);
        assert_eq!(count(t), 3);

        // DELETE specific member (SREM)
        Spi::run(&format!("DELETE FROM {t} WHERE member = 'apple';")).unwrap();
        assert_eq!(count(t), 2);

        // DELETE remaining
        Spi::run(&format!("DELETE FROM {t} WHERE member = 'cherry';")).unwrap();
        Spi::run(&format!("DELETE FROM {t} WHERE member = 'date';")).unwrap();
        assert_eq!(count(t), 0);

        drop_table(t);
        teardown();
    }

    #[pg_test]
    fn test_streaming_set_duplicate_insert() {
        setup();
        let t = "sm_set_dup";
        let k = "sm:set:dup";
        create_table(t, "member TEXT", "set", k);

        Spi::run(&format!("INSERT INTO {t} (member) VALUES ('unique');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} (member) VALUES ('unique');")).unwrap();

        // Sets deduplicate — should still be 1
        assert_eq!(count(t), 1);

        Spi::run(&format!("DELETE FROM {t} WHERE member = 'unique';")).unwrap();
        drop_table(t);
        teardown();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ZSET: SELECT, INSERT, UPDATE, DELETE
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    fn test_streaming_zset_full_crud() {
        setup();
        let t = "sm_zset";
        let k = "sm:zset:crud";
        create_table(t, "member TEXT, score TEXT", "zset", k);

        // INSERT with scores (ZADD)
        Spi::run(&format!("INSERT INTO {t} VALUES ('alice', '100');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} VALUES ('bob', '200');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} VALUES ('carol', '150');")).unwrap();
        assert_eq!(count(t), 3);

        // SELECT with pushdown
        assert_eq!(
            get_one(&format!("SELECT score FROM {t} WHERE member = 'bob';")),
            "200"
        );

        // UPDATE score
        Spi::run(&format!(
            "UPDATE {t} SET score = '999' WHERE member = 'alice';"
        ))
        .unwrap();
        assert_eq!(
            get_one(&format!("SELECT score FROM {t} WHERE member = 'alice';")),
            "999"
        );

        // UPDATE member rename (ZREM old + ZADD new)
        Spi::run(&format!(
            "UPDATE {t} SET member = 'dave', score = '150' WHERE member = 'carol';"
        ))
        .unwrap();
        assert_eq!(count_where(t, "member = 'carol'"), 0);
        assert_eq!(count_where(t, "member = 'dave'"), 1);

        // DELETE specific member (ZREM)
        Spi::run(&format!("DELETE FROM {t} WHERE member = 'bob';")).unwrap();
        assert_eq!(count(t), 2);

        // DELETE remaining
        Spi::run(&format!("DELETE FROM {t} WHERE member = 'alice';")).unwrap();
        Spi::run(&format!("DELETE FROM {t} WHERE member = 'dave';")).unwrap();
        assert_eq!(count(t), 0);

        drop_table(t);
        teardown();
    }

    #[pg_test]
    fn test_streaming_zset_score_ordering() {
        setup();
        let t = "sm_zset_ord";
        let k = "sm:zset:ord";
        create_table(t, "member TEXT, score TEXT", "zset", k);

        Spi::run(&format!("INSERT INTO {t} VALUES ('low', '10');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} VALUES ('mid', '50');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} VALUES ('high', '90');")).unwrap();

        // ZSet returns data sorted by score — first row should be lowest score
        let first_member = get_one(&format!("SELECT member FROM {t} LIMIT 1;"));
        assert_eq!(first_member, "low");

        Spi::run(&format!("DELETE FROM {t} WHERE member = 'low';")).unwrap();
        Spi::run(&format!("DELETE FROM {t} WHERE member = 'mid';")).unwrap();
        Spi::run(&format!("DELETE FROM {t} WHERE member = 'high';")).unwrap();

        drop_table(t);
        teardown();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // STREAM: SELECT, INSERT, DELETE (UPDATE is NOT supported)
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    fn test_streaming_stream_insert_select_delete() {
        setup();
        let t = "sm_stream";
        let k = "sm:stream:crud";
        create_table(t, "id TEXT, field TEXT, value TEXT", "stream", k);

        // INSERT entries (XADD with auto-generated IDs)
        Spi::run(&format!(
            "INSERT INTO {t} VALUES ('*', 'sensor', 'temp_22');"
        ))
        .unwrap();
        Spi::run(&format!(
            "INSERT INTO {t} VALUES ('*', 'sensor', 'temp_23');"
        ))
        .unwrap();
        Spi::run(&format!(
            "INSERT INTO {t} VALUES ('*', 'sensor', 'temp_24');"
        ))
        .unwrap();

        // SELECT — stream should have 3 entries
        assert_eq!(count(t), 3);

        // Get an ID from the stream for targeted DELETE
        let stream_id = get_one(&format!("SELECT id FROM {t} LIMIT 1;"));
        assert!(!stream_id.is_empty());
        assert!(stream_id.contains('-'));

        // DELETE by stream ID (XDEL)
        Spi::run(&format!("DELETE FROM {t} WHERE id = '{stream_id}';")).unwrap();
        assert_eq!(count(t), 2);

        // Clean up remaining entries
        let id2 = get_one(&format!("SELECT id FROM {t} LIMIT 1;"));
        Spi::run(&format!("DELETE FROM {t} WHERE id = '{id2}';")).unwrap();
        let id3 = get_one(&format!("SELECT id FROM {t} LIMIT 1;"));
        Spi::run(&format!("DELETE FROM {t} WHERE id = '{id3}';")).unwrap();
        assert_eq!(count(t), 0);

        drop_table(t);
        teardown();
    }

    #[pg_test]
    fn test_streaming_stream_update_rejected() {
        // Verify that UPDATE on a stream is rejected at the Rust trait level.
        // At the SQL level, IsForeignRelUpdatable returns a bitmask without UPDATE,
        // so PostgreSQL raises ERROR "does not allow updates" before reaching our code.
        // We test the trait-level rejection instead.
        use crate::tables::implementations::RedisStreamTable;
        use crate::tables::interface::RedisTableOperations;

        let mut stream_table = RedisStreamTable::new(100);
        let mut conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        let result = stream_table.update(
            &mut conn,
            "sm_stream_update_test",
            &["old".to_string()],
            &["new".to_string()],
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("UPDATE is not supported for Redis Stream"));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // CROSS-TYPE: Exercises memory safety under repeated operations
    // ═══════════════════════════════════════════════════════════════════════

    #[pg_test]
    fn test_streaming_multiple_modify_cycles() {
        setup();
        let t = "sm_multi_hash";
        let k = "sm:multi:cycles";
        create_table(t, "field TEXT, value TEXT", "hash", k);

        // Repeated INSERT → SELECT → UPDATE → DELETE cycles
        // Each cycle exercises the full plan_foreign_modify → begin → exec → end path
        for i in 0..5 {
            let field = format!("key_{i}");
            let val = format!("val_{i}");
            let updated_val = format!("updated_{i}");

            Spi::run(&format!("INSERT INTO {t} VALUES ('{field}', '{val}');")).unwrap();
            assert_eq!(
                get_one(&format!("SELECT value FROM {t} WHERE field = '{field}';")),
                val
            );

            Spi::run(&format!(
                "UPDATE {t} SET value = '{updated_val}' WHERE field = '{field}';"
            ))
            .unwrap();
            assert_eq!(
                get_one(&format!("SELECT value FROM {t} WHERE field = '{field}';")),
                updated_val
            );

            Spi::run(&format!("DELETE FROM {t} WHERE field = '{field}';")).unwrap();
            assert_eq!(count_where(t, &format!("field = '{field}'")), 0);
        }

        assert_eq!(count(t), 0);
        drop_table(t);
        teardown();
    }

    #[pg_test]
    fn test_streaming_rescan_correctness() {
        setup();
        let t_hash = "sm_rescan_hash";
        let k_hash = "sm:rescan:hash";
        create_table(t_hash, "field TEXT, value TEXT", "hash", k_hash);

        // Insert data
        Spi::run(&format!("INSERT INTO {t_hash} VALUES ('x', '1');")).unwrap();
        Spi::run(&format!("INSERT INTO {t_hash} VALUES ('y', '2');")).unwrap();
        Spi::run(&format!("INSERT INTO {t_hash} VALUES ('z', '3');")).unwrap();

        // Multiple SELECTs to trigger rescan paths
        assert_eq!(count(t_hash), 3);
        assert_eq!(count(t_hash), 3);
        assert_eq!(count_where(t_hash, "field = 'x'"), 1);
        assert_eq!(count_where(t_hash, "field = 'y'"), 1);

        // Modify between selects — exercises rescan state reset
        Spi::run(&format!(
            "UPDATE {t_hash} SET value = '99' WHERE field = 'x';"
        ))
        .unwrap();
        assert_eq!(
            get_one(&format!("SELECT value FROM {t_hash} WHERE field = 'x';")),
            "99"
        );
        assert_eq!(count(t_hash), 3);

        // Clean up
        Spi::run(&format!("DELETE FROM {t_hash} WHERE field = 'x';")).unwrap();
        Spi::run(&format!("DELETE FROM {t_hash} WHERE field = 'y';")).unwrap();
        Spi::run(&format!("DELETE FROM {t_hash} WHERE field = 'z';")).unwrap();

        drop_table(t_hash);
        teardown();
    }

    #[pg_test]
    fn test_streaming_all_types_sequential_modify() {
        setup();

        // Create tables for all types
        create_table("sm_all_str", "value TEXT", "string", "sm:all:str");
        create_table(
            "sm_all_hash",
            "field TEXT, value TEXT",
            "hash",
            "sm:all:hash",
        );
        create_table("sm_all_list", "value TEXT", "list", "sm:all:list");
        create_table("sm_all_set", "member TEXT", "set", "sm:all:set");
        create_table(
            "sm_all_zset",
            "member TEXT, score TEXT",
            "zset",
            "sm:all:zset",
        );
        create_table(
            "sm_all_stream",
            "id TEXT, field TEXT, value TEXT",
            "stream",
            "sm:all:stream",
        );

        // INSERT into each type
        Spi::run("INSERT INTO sm_all_str (value) VALUES ('hello');").unwrap();
        Spi::run("INSERT INTO sm_all_hash VALUES ('k1', 'v1');").unwrap();
        Spi::run("INSERT INTO sm_all_list (value) VALUES ('item1');").unwrap();
        Spi::run("INSERT INTO sm_all_set (member) VALUES ('m1');").unwrap();
        Spi::run("INSERT INTO sm_all_zset VALUES ('p1', '100');").unwrap();
        Spi::run("INSERT INTO sm_all_stream VALUES ('*', 'sensor', 'data1');").unwrap();

        // SELECT from each type
        assert_eq!(count("sm_all_str"), 1);
        assert_eq!(count("sm_all_hash"), 1);
        assert_eq!(count("sm_all_list"), 1);
        assert_eq!(count("sm_all_set"), 1);
        assert_eq!(count("sm_all_zset"), 1);
        assert_eq!(count("sm_all_stream"), 1);

        // UPDATE each (except stream)
        Spi::run("UPDATE sm_all_str SET value = 'world';").unwrap();
        Spi::run("UPDATE sm_all_hash SET value = 'v2' WHERE field = 'k1';").unwrap();
        Spi::run("UPDATE sm_all_list SET value = 'item2' WHERE value = 'item1';").unwrap();
        Spi::run("UPDATE sm_all_set SET member = 'm2' WHERE member = 'm1';").unwrap();
        Spi::run("UPDATE sm_all_zset SET score = '200' WHERE member = 'p1';").unwrap();

        // Verify updates
        assert_eq!(get_one("SELECT value FROM sm_all_str;"), "world");
        assert_eq!(
            get_one("SELECT value FROM sm_all_hash WHERE field = 'k1';"),
            "v2"
        );
        assert_eq!(count_where("sm_all_list", "value = 'item2'"), 1);
        assert_eq!(count_where("sm_all_set", "member = 'm2'"), 1);
        assert_eq!(
            get_one("SELECT score FROM sm_all_zset WHERE member = 'p1';"),
            "200"
        );

        // DELETE from each type
        Spi::run("DELETE FROM sm_all_str;").unwrap();
        Spi::run("DELETE FROM sm_all_hash WHERE field = 'k1';").unwrap();
        Spi::run("DELETE FROM sm_all_list WHERE value = 'item2';").unwrap();
        Spi::run("DELETE FROM sm_all_set WHERE member = 'm2';").unwrap();
        Spi::run("DELETE FROM sm_all_zset WHERE member = 'p1';").unwrap();
        // Stream: get ID then delete
        let stream_id = get_one("SELECT id FROM sm_all_stream LIMIT 1;");
        Spi::run(&format!(
            "DELETE FROM sm_all_stream WHERE id = '{stream_id}';"
        ))
        .unwrap();

        // Verify all empty
        assert_eq!(count("sm_all_str"), 0);
        assert_eq!(count("sm_all_hash"), 0);
        assert_eq!(count("sm_all_list"), 0);
        assert_eq!(count("sm_all_set"), 0);
        assert_eq!(count("sm_all_zset"), 0);
        assert_eq!(count("sm_all_stream"), 0);

        // DROP all
        drop_table("sm_all_str");
        drop_table("sm_all_hash");
        drop_table("sm_all_list");
        drop_table("sm_all_set");
        drop_table("sm_all_zset");
        drop_table("sm_all_stream");
        teardown();
    }

    #[pg_test]
    fn test_streaming_scan_complete_flag() {
        setup();
        let t = "sm_scan_flag";
        let k = "sm:scan:flag";
        create_table(t, "member TEXT", "set", k);

        // Insert a known number of items and verify exact count
        // This exercises the iterate_foreign_scan → is_read_end → scan_complete path
        Spi::run(&format!("INSERT INTO {t} (member) VALUES ('a');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} (member) VALUES ('b');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} (member) VALUES ('c');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} (member) VALUES ('d');")).unwrap();
        Spi::run(&format!("INSERT INTO {t} (member) VALUES ('e');")).unwrap();

        // COUNT triggers full iteration — scan_complete must be set correctly
        assert_eq!(count(t), 5);

        // A second COUNT verifies the state is properly reset between scans
        assert_eq!(count(t), 5);

        // Cleanup
        Spi::run(&format!("DELETE FROM {t} WHERE member = 'a';")).unwrap();
        Spi::run(&format!("DELETE FROM {t} WHERE member = 'b';")).unwrap();
        Spi::run(&format!("DELETE FROM {t} WHERE member = 'c';")).unwrap();
        Spi::run(&format!("DELETE FROM {t} WHERE member = 'd';")).unwrap();
        Spi::run(&format!("DELETE FROM {t} WHERE member = 'e';")).unwrap();

        drop_table(t);
        teardown();
    }

    #[pg_test]
    fn test_streaming_empty_table_operations() {
        setup();
        let t = "sm_empty";
        let k = "sm:empty:ops";
        create_table(t, "value TEXT", "list", k);

        // SELECT on empty — should return 0 without error
        assert_eq!(count(t), 0);

        // DELETE on empty — should not error
        let result = Spi::run(&format!("DELETE FROM {t} WHERE value = 'nonexist';"));
        assert!(result.is_ok());

        drop_table(t);
        teardown();
    }
}
