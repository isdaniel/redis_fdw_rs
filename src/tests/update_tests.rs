#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    const REDIS_HOST_PORT: &str = "127.0.0.1:8899";
    const TEST_DATABASE: &str = "15";
    const FDW_NAME: &str = "redis_update_test_wrapper";
    const SERVER_NAME: &str = "redis_update_test_server";

    fn setup_redis_fdw() {
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

    fn cleanup_redis_fdw() {
        let _ = Spi::run(&format!("DROP SERVER IF EXISTS {} CASCADE;", SERVER_NAME));
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            FDW_NAME
        ));
    }

    fn create_foreign_table(table_name: &str, columns: &str, table_type: &str, key_prefix: &str) {
        let sql = format!(
            "CREATE FOREIGN TABLE {table_name} ({columns}) SERVER {SERVER_NAME} OPTIONS (
                database '{TEST_DATABASE}',
                table_type '{table_type}',
                table_key_prefix '{key_prefix}'
            );"
        );
        Spi::run(&sql).unwrap();
    }

    // --- String UPDATE Tests ---

    #[pg_test]
    fn test_string_update() {
        setup_redis_fdw();

        create_foreign_table(
            "test_string_upd",
            "value text",
            "string",
            "test_str_update_key",
        );

        Spi::run("INSERT INTO test_string_upd (value) VALUES ('original');").unwrap();

        let original = Spi::get_one::<String>("SELECT value FROM test_string_upd;")
            .unwrap()
            .unwrap();
        assert_eq!(original, "original");

        Spi::run("UPDATE test_string_upd SET value = 'updated';").unwrap();

        let updated = Spi::get_one::<String>("SELECT value FROM test_string_upd;")
            .unwrap()
            .unwrap();
        assert_eq!(updated, "updated");

        Spi::run("DELETE FROM test_string_upd;").unwrap();
        cleanup_redis_fdw();
    }

    // --- Hash UPDATE Tests ---

    #[pg_test]
    fn test_hash_update_value() {
        setup_redis_fdw();

        create_foreign_table(
            "test_hash_upd",
            "key text, value text",
            "hash",
            "test_hash_update_key",
        );

        Spi::run("INSERT INTO test_hash_upd (key, value) VALUES ('field1', 'value1');").unwrap();

        let original =
            Spi::get_one::<String>("SELECT value FROM test_hash_upd WHERE key = 'field1';")
                .unwrap()
                .unwrap();
        assert_eq!(original, "value1");

        Spi::run("UPDATE test_hash_upd SET value = 'new_value' WHERE key = 'field1';").unwrap();

        let updated =
            Spi::get_one::<String>("SELECT value FROM test_hash_upd WHERE key = 'field1';")
                .unwrap()
                .unwrap();
        assert_eq!(updated, "new_value");

        Spi::run("DELETE FROM test_hash_upd WHERE key = 'field1';").unwrap();
        cleanup_redis_fdw();
    }

    #[pg_test]
    fn test_hash_update_field_rename() {
        setup_redis_fdw();

        create_foreign_table(
            "test_hash_upd_rename",
            "key text, value text",
            "hash",
            "test_hash_update_rename_key",
        );

        Spi::run("INSERT INTO test_hash_upd_rename (key, value) VALUES ('old_field', 'data');")
            .unwrap();

        // Rename field: old_field -> new_field (should HDEL old, HSET new)
        Spi::run(
            "UPDATE test_hash_upd_rename SET key = 'new_field', value = 'data' WHERE key = 'old_field';",
        )
        .unwrap();

        let old_count = Spi::get_one::<i64>(
            "SELECT count(*) FROM test_hash_upd_rename WHERE key = 'old_field';",
        )
        .unwrap()
        .unwrap();
        assert_eq!(old_count, 0);

        let new_value = Spi::get_one::<String>(
            "SELECT value FROM test_hash_upd_rename WHERE key = 'new_field';",
        )
        .unwrap()
        .unwrap();
        assert_eq!(new_value, "data");

        Spi::run("DELETE FROM test_hash_upd_rename WHERE key = 'new_field';").unwrap();
        cleanup_redis_fdw();
    }

    // --- Set UPDATE Tests ---

    #[pg_test]
    fn test_set_update_member() {
        setup_redis_fdw();

        create_foreign_table("test_set_upd", "member text", "set", "test_set_update_key");

        Spi::run("INSERT INTO test_set_upd (member) VALUES ('old_member');").unwrap();

        let count =
            Spi::get_one::<i64>("SELECT count(*) FROM test_set_upd WHERE member = 'old_member';")
                .unwrap()
                .unwrap();
        assert_eq!(count, 1);

        Spi::run("UPDATE test_set_upd SET member = 'new_member' WHERE member = 'old_member';")
            .unwrap();

        let old_count =
            Spi::get_one::<i64>("SELECT count(*) FROM test_set_upd WHERE member = 'old_member';")
                .unwrap()
                .unwrap();
        assert_eq!(old_count, 0);

        let new_count =
            Spi::get_one::<i64>("SELECT count(*) FROM test_set_upd WHERE member = 'new_member';")
                .unwrap()
                .unwrap();
        assert_eq!(new_count, 1);

        Spi::run("DELETE FROM test_set_upd WHERE member = 'new_member';").unwrap();
        cleanup_redis_fdw();
    }

    // --- ZSet UPDATE Tests ---

    #[pg_test]
    fn test_zset_update_score() {
        setup_redis_fdw();

        create_foreign_table(
            "test_zset_upd_score",
            "member text, score text",
            "zset",
            "test_zset_update_score_key",
        );

        Spi::run("INSERT INTO test_zset_upd_score (member, score) VALUES ('player1', '100');")
            .unwrap();

        let original_score = Spi::get_one::<String>(
            "SELECT score FROM test_zset_upd_score WHERE member = 'player1';",
        )
        .unwrap()
        .unwrap();
        assert_eq!(original_score, "100");

        Spi::run("UPDATE test_zset_upd_score SET score = '200' WHERE member = 'player1';").unwrap();

        let updated_score = Spi::get_one::<String>(
            "SELECT score FROM test_zset_upd_score WHERE member = 'player1';",
        )
        .unwrap()
        .unwrap();
        assert_eq!(updated_score, "200");

        Spi::run("DELETE FROM test_zset_upd_score WHERE member = 'player1';").unwrap();
        cleanup_redis_fdw();
    }

    #[pg_test]
    fn test_zset_update_member() {
        setup_redis_fdw();

        create_foreign_table(
            "test_zset_upd_member",
            "member text, score text",
            "zset",
            "test_zset_update_member_key",
        );

        Spi::run("INSERT INTO test_zset_upd_member (member, score) VALUES ('old_player', '50');")
            .unwrap();

        Spi::run(
            "UPDATE test_zset_upd_member SET member = 'new_player', score = '50' WHERE member = 'old_player';",
        )
        .unwrap();

        let old_count = Spi::get_one::<i64>(
            "SELECT count(*) FROM test_zset_upd_member WHERE member = 'old_player';",
        )
        .unwrap()
        .unwrap();
        assert_eq!(old_count, 0);

        let new_count = Spi::get_one::<i64>(
            "SELECT count(*) FROM test_zset_upd_member WHERE member = 'new_player';",
        )
        .unwrap()
        .unwrap();
        assert_eq!(new_count, 1);

        Spi::run("DELETE FROM test_zset_upd_member WHERE member = 'new_player';").unwrap();
        cleanup_redis_fdw();
    }

    // --- List UPDATE Tests ---

    #[pg_test]
    fn test_list_update_element() {
        setup_redis_fdw();

        create_foreign_table(
            "test_list_upd",
            "value text",
            "list",
            "test_list_update_key",
        );

        Spi::run("INSERT INTO test_list_upd (value) VALUES ('item_a');").unwrap();
        Spi::run("INSERT INTO test_list_upd (value) VALUES ('item_b');").unwrap();

        Spi::run("UPDATE test_list_upd SET value = 'item_c' WHERE value = 'item_a';").unwrap();

        let count_old =
            Spi::get_one::<i64>("SELECT count(*) FROM test_list_upd WHERE value = 'item_a';")
                .unwrap()
                .unwrap();
        assert_eq!(count_old, 0);

        let count_new =
            Spi::get_one::<i64>("SELECT count(*) FROM test_list_upd WHERE value = 'item_c';")
                .unwrap()
                .unwrap();
        assert_eq!(count_new, 1);

        Spi::run("DELETE FROM test_list_upd;").unwrap();
        cleanup_redis_fdw();
    }

    // --- Stream UPDATE Tests ---
    // Note: Stream UPDATE is blocked at the PostgreSQL planner level by IsForeignRelUpdatable
    // which returns a bitmask without the UPDATE bit. This is tested via the
    // test_stream_update_trait test below which validates the Rust-level behavior.

    #[pg_test]
    fn test_stream_update_trait() {
        use crate::tables::implementations::RedisStreamTable;
        use crate::tables::interface::RedisTableOperations;

        // Verify that RedisStreamTable's update method returns an error
        let mut stream_table = RedisStreamTable::new(100);
        let mut mock_conn = redis::Client::open("redis://127.0.0.1:8899/15")
            .unwrap()
            .get_connection()
            .unwrap();
        let result = stream_table.update(
            &mut mock_conn,
            "test_stream_key",
            &["old".to_string()],
            &["new".to_string()],
        );
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("UPDATE is not supported for Redis Stream"),
            "Expected stream update error, got: {}",
            err_msg
        );
    }
}
