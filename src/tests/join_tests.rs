#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    const REDIS_HOST_PORT: &str = "127.0.0.1:8899";
    const TEST_DATABASE: &str = "15";
    const FDW_NAME: &str = "redis_join_wrapper";
    const SERVER_NAME: &str = "redis_join_server";

    fn setup_join_fdw() {
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

    fn cleanup_join_fdw() {
        let _ = Spi::run(&format!("DROP SERVER IF EXISTS {} CASCADE;", SERVER_NAME));
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            FDW_NAME
        ));
    }

    fn create_join_table(table_name: &str, columns: &str, table_type: &str, key_prefix: &str) {
        let sql = format!(
            "CREATE FOREIGN TABLE {table_name} ({columns}) SERVER {SERVER_NAME} OPTIONS (
                database '{TEST_DATABASE}',
                table_type '{table_type}',
                table_key_prefix '{key_prefix}'
            );"
        );
        Spi::run(&sql).unwrap();
    }

    #[pg_test]
    fn test_join_hash_with_local_table() {
        setup_join_fdw();

        Spi::run("CREATE TEMPORARY TABLE join_local (id text, name text);").unwrap();
        Spi::run("INSERT INTO join_local VALUES ('a', 'Alice'), ('b', 'Bob'), ('c', 'Charlie');")
            .unwrap();

        let table_name = "join_hash_local";
        let key_prefix = "join_test:hash_local";
        create_join_table(table_name, "field text, value text", "hash", key_prefix);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('a', 'val_a'), ('b', 'val_b'), ('d', 'val_d');",
            table_name
        ))
        .unwrap();

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_local l JOIN {} r ON l.id = r.field;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(count, 2, "INNER JOIN should find 2 matches (a, b)");

        let left_count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_local l LEFT JOIN {} r ON l.id = r.field;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(left_count, 3, "LEFT JOIN should return all 3 local rows");

        Spi::run(&format!("DELETE FROM {} WHERE field = 'a';", table_name)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'b';", table_name)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'd';", table_name)).unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        let _ = Spi::run("DROP TABLE IF EXISTS join_local;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_set_with_local_table() {
        setup_join_fdw();

        Spi::run("CREATE TEMPORARY TABLE join_local_set (id text);").unwrap();
        Spi::run("INSERT INTO join_local_set VALUES ('member1'), ('member2'), ('missing');")
            .unwrap();

        let table_name = "join_set_local";
        let key_prefix = "join_test:set_local";
        create_join_table(table_name, "member text", "set", key_prefix);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('member1'), ('member2'), ('member3');",
            table_name
        ))
        .unwrap();

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_local_set l JOIN {} r ON l.id = r.member;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(count, 2, "Should match member1 and member2");

        Spi::run(&format!(
            "DELETE FROM {} WHERE member = 'member1';",
            table_name
        ))
        .unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE member = 'member2';",
            table_name
        ))
        .unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE member = 'member3';",
            table_name
        ))
        .unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        let _ = Spi::run("DROP TABLE IF EXISTS join_local_set;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_two_redis_tables() {
        setup_join_fdw();

        let hash_table = "join_fdw_hash";
        let set_table = "join_fdw_set";
        let hash_key = "join_test:fdw_hash";
        let set_key = "join_test:fdw_set";

        create_join_table(hash_table, "field text, value text", "hash", hash_key);
        create_join_table(set_table, "member text", "set", set_key);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('x', 'val_x'), ('y', 'val_y'), ('z', 'val_z');",
            hash_table
        ))
        .unwrap();
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('x'), ('y'), ('w');",
            set_table
        ))
        .unwrap();

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM {} h JOIN {} s ON h.field = s.member;",
            hash_table, set_table
        ))
        .unwrap()
        .unwrap();

        assert_eq!(count, 2, "FDW-to-FDW join should find 2 matches (x, y)");

        Spi::run(&format!("DELETE FROM {} WHERE field = 'x';", hash_table)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'y';", hash_table)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'z';", hash_table)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE member = 'x';", set_table)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE member = 'y';", set_table)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE member = 'w';", set_table)).unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", hash_table));
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", set_table));
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_with_where_pushdown() {
        setup_join_fdw();

        Spi::run("CREATE TEMPORARY TABLE join_where_local (id text);").unwrap();
        Spi::run("INSERT INTO join_where_local VALUES ('field1'), ('field2'), ('field3');")
            .unwrap();

        let table_name = "join_where_hash";
        let key_prefix = "join_test:where_hash";
        create_join_table(table_name, "field text, value text", "hash", key_prefix);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('field1', 'A'), ('field2', 'B'), ('field3', 'C');",
            table_name
        ))
        .unwrap();

        // JOIN + WHERE pushdown on the field column (valid pushdown for hash)
        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_where_local l JOIN {} r ON l.id = r.field WHERE r.field = 'field1';",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(count, 1, "Should match only field1 via pushdown");

        Spi::run(&format!(
            "DELETE FROM {} WHERE field = 'field1';",
            table_name
        ))
        .unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE field = 'field2';",
            table_name
        ))
        .unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE field = 'field3';",
            table_name
        ))
        .unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        let _ = Spi::run("DROP TABLE IF EXISTS join_where_local;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_left_join_null_padding() {
        setup_join_fdw();

        Spi::run("CREATE TEMPORARY TABLE join_left_local (id text, label text);").unwrap();
        Spi::run("INSERT INTO join_left_local VALUES ('match', 'yes'), ('nomatch', 'no');")
            .unwrap();

        let table_name = "join_left_hash";
        let key_prefix = "join_test:left_hash";
        create_join_table(table_name, "field text, value text", "hash", key_prefix);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('match', 'found');",
            table_name
        ))
        .unwrap();

        let null_count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_left_local l LEFT JOIN {} r ON l.id = r.field WHERE r.field IS NULL;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            null_count, 1,
            "One row should have NULL from unmatched LEFT JOIN"
        );

        Spi::run(&format!(
            "DELETE FROM {} WHERE field = 'match';",
            table_name
        ))
        .unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        let _ = Spi::run("DROP TABLE IF EXISTS join_left_local;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_zset_on_member_column() {
        setup_join_fdw();

        // ZSet standard columns are (member text, score text) — member is column index 0
        let zset_table = "join_zset_member";
        let zset_key = "join_test:zset_member";
        create_join_table(zset_table, "member text, score text", "zset", zset_key);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('player_a', 100), ('player_b', 200), ('player_c', 300);",
            zset_table
        ))
        .unwrap();

        // Local table to join on
        Spi::run("CREATE TEMPORARY TABLE join_zset_local (name text, rank int);").unwrap();
        Spi::run(
            "INSERT INTO join_zset_local VALUES ('player_a', 1), ('player_b', 2), ('player_x', 3);",
        )
        .unwrap();

        // JOIN on member (column 0 of zset) = name (column 0 of local)
        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_zset_local l JOIN {} r ON l.name = r.member;",
            zset_table
        ))
        .unwrap()
        .unwrap();

        assert_eq!(count, 2, "Should match player_a and player_b");

        Spi::run(&format!(
            "DELETE FROM {} WHERE member = 'player_a';",
            zset_table
        ))
        .unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE member = 'player_b';",
            zset_table
        ))
        .unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE member = 'player_c';",
            zset_table
        ))
        .unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", zset_table));
        let _ = Spi::run("DROP TABLE IF EXISTS join_zset_local;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_fdw_hash_to_zset() {
        setup_join_fdw();

        let hash_table = "join_fdw_h2z_hash";
        let zset_table = "join_fdw_h2z_zset";
        let hash_key = "join_test:h2z_hash";
        let zset_key = "join_test:h2z_zset";

        create_join_table(hash_table, "field text, value text", "hash", hash_key);
        create_join_table(zset_table, "member text, score text", "zset", zset_key);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('alpha', 'data_a'), ('beta', 'data_b'), ('gamma', 'data_g');",
            hash_table
        ))
        .unwrap();
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('alpha', 10), ('beta', 20), ('delta', 30);",
            zset_table
        ))
        .unwrap();

        // FDW-to-FDW join: hash.field = zset.member (column 0 = column 0)
        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM {} h JOIN {} z ON h.field = z.member;",
            hash_table, zset_table
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            count, 2,
            "FDW-to-FDW hash.field=zset.member should find alpha, beta"
        );

        Spi::run(&format!(
            "DELETE FROM {} WHERE field = 'alpha';",
            hash_table
        ))
        .unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'beta';", hash_table)).unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE field = 'gamma';",
            hash_table
        ))
        .unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE member = 'alpha';",
            zset_table
        ))
        .unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE member = 'beta';",
            zset_table
        ))
        .unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE member = 'delta';",
            zset_table
        ))
        .unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", hash_table));
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", zset_table));
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_empty_table() {
        setup_join_fdw();

        let table_name = "join_empty_hash";
        let key_prefix = "join_test:empty_hash";
        create_join_table(table_name, "field text, value text", "hash", key_prefix);

        // Don't insert any data — table is empty

        Spi::run("CREATE TEMPORARY TABLE join_empty_local (id text);").unwrap();
        Spi::run("INSERT INTO join_empty_local VALUES ('a'), ('b');").unwrap();

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_empty_local l JOIN {} r ON l.id = r.field;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(count, 0, "JOIN with empty Redis table should return 0 rows");

        let left_count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_empty_local l LEFT JOIN {} r ON l.id = r.field;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            left_count, 2,
            "LEFT JOIN with empty Redis table should return all local rows"
        );

        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        let _ = Spi::run("DROP TABLE IF EXISTS join_empty_local;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_explain_output() {
        setup_join_fdw();

        let hash_table = "join_explain_hash";
        let set_table = "join_explain_set";
        let hash_key = "join_test:explain_hash";
        let set_key = "join_test:explain_set";

        create_join_table(hash_table, "field text, value text", "hash", hash_key);
        create_join_table(set_table, "member text", "set", set_key);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('x', 'val_x');",
            hash_table
        ))
        .unwrap();
        Spi::run(&format!("INSERT INTO {} VALUES ('x');", set_table)).unwrap();

        // Collect full EXPLAIN output across all rows
        let explain_rows: Vec<String> = Spi::connect(|client| {
            let mut rows = Vec::new();
            let query = format!(
                "EXPLAIN (FORMAT TEXT) SELECT * FROM {} h JOIN {} s ON h.field = s.member;",
                hash_table, set_table
            );
            let tup_table = client.select(&query, None, &[]).unwrap();
            for row in tup_table {
                if let Some(line) = row.get::<String>(1).unwrap_or(None) {
                    rows.push(line);
                }
            }
            rows
        });

        let full_explain = explain_rows.join("\n");
        log!("Full EXPLAIN output:\n{}", full_explain);

        // Verify plan contains Foreign Scan (pushdown) on the join relation
        let has_foreign_scan = full_explain.contains("Foreign Scan");
        let has_nested_loop = full_explain.contains("Nested Loop");
        assert!(
            has_foreign_scan || has_nested_loop,
            "EXPLAIN should show Foreign Scan (pushdown) or Nested Loop, got:\n{}",
            full_explain
        );

        Spi::run(&format!("DELETE FROM {} WHERE field = 'x';", hash_table)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE member = 'x';", set_table)).unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", hash_table));
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", set_table));
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_content_verification() {
        setup_join_fdw();

        let hash_table = "join_content_hash";
        let hash_key = "join_test:content_hash";
        create_join_table(hash_table, "field text, value text", "hash", hash_key);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('key1', 'hello'), ('key2', 'world');",
            hash_table
        ))
        .unwrap();

        Spi::run("CREATE TEMPORARY TABLE join_content_local (id text, label text);").unwrap();
        Spi::run("INSERT INTO join_content_local VALUES ('key1', 'first'), ('key2', 'second');")
            .unwrap();

        // Verify actual content of joined rows, not just count
        let result = Spi::get_one::<String>(&format!(
            "SELECT r.value FROM join_content_local l JOIN {} r ON l.id = r.field WHERE l.id = 'key1';",
            hash_table
        ))
        .unwrap()
        .unwrap();

        assert_eq!(result, "hello", "JOIN should return correct value for key1");

        let result2 = Spi::get_one::<String>(&format!(
            "SELECT l.label FROM join_content_local l JOIN {} r ON l.id = r.field WHERE r.field = 'key2';",
            hash_table
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            result2, "second",
            "JOIN should return correct local column for key2"
        );

        Spi::run(&format!("DELETE FROM {} WHERE field = 'key1';", hash_table)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'key2';", hash_table)).unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", hash_table));
        let _ = Spi::run("DROP TABLE IF EXISTS join_content_local;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_left_join_content_with_nulls() {
        setup_join_fdw();

        let hash_table = "join_nullcontent_hash";
        let hash_key = "join_test:nullcontent_hash";
        create_join_table(hash_table, "field text, value text", "hash", hash_key);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('a', 'found_a');",
            hash_table
        ))
        .unwrap();

        Spi::run("CREATE TEMPORARY TABLE join_nullcontent_local (id text);").unwrap();
        Spi::run("INSERT INTO join_nullcontent_local VALUES ('a'), ('missing');").unwrap();

        // LEFT JOIN: 'missing' should produce NULL for the Redis side
        let null_value = Spi::get_one::<String>(&format!(
            "SELECT r.value FROM join_nullcontent_local l LEFT JOIN {} r ON l.id = r.field WHERE l.id = 'missing';",
            hash_table
        ))
        .unwrap();

        assert!(
            null_value.is_none(),
            "LEFT JOIN with no match should produce NULL, got: {:?}",
            null_value
        );

        let found_value = Spi::get_one::<String>(&format!(
            "SELECT r.value FROM join_nullcontent_local l LEFT JOIN {} r ON l.id = r.field WHERE l.id = 'a';",
            hash_table
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            found_value, "found_a",
            "LEFT JOIN with match should return the value"
        );

        Spi::run(&format!("DELETE FROM {} WHERE field = 'a';", hash_table)).unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", hash_table));
        let _ = Spi::run("DROP TABLE IF EXISTS join_nullcontent_local;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_list_with_local_table() {
        setup_join_fdw();

        Spi::run("CREATE TEMPORARY TABLE join_list_local (val text);").unwrap();
        Spi::run("INSERT INTO join_list_local VALUES ('item1'), ('item2'), ('missing');").unwrap();

        let table_name = "join_list_test";
        let key_prefix = "join_test:list_local";
        create_join_table(table_name, "element text", "list", key_prefix);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('item1'), ('item2'), ('item3');",
            table_name
        ))
        .unwrap();

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_list_local l JOIN {} r ON l.val = r.element;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(count, 2, "List JOIN should match item1 and item2");

        Spi::run(&format!(
            "DELETE FROM {} WHERE element = 'item1';",
            table_name
        ))
        .unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE element = 'item2';",
            table_name
        ))
        .unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE element = 'item3';",
            table_name
        ))
        .unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        let _ = Spi::run("DROP TABLE IF EXISTS join_list_local;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_string_with_local_table() {
        setup_join_fdw();

        // String type uses multi-key pattern (key*, with key column + value column)
        let table_name = "join_string_test";
        let key_prefix = "join_test:str:*";
        create_join_table(table_name, "key text, value text", "string", key_prefix);

        // Insert data directly via Redis commands using a helper hash table
        // For string multi-key: first column is key name, second is value
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('join_test:str:alpha', 'val_alpha'), ('join_test:str:beta', 'val_beta');",
            table_name
        ))
        .unwrap();

        Spi::run("CREATE TEMPORARY TABLE join_str_local (k text);").unwrap();
        Spi::run(
            "INSERT INTO join_str_local VALUES ('join_test:str:alpha'), ('join_test:str:gamma');",
        )
        .unwrap();

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_str_local l JOIN {} r ON l.k = r.key;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(count, 1, "String multi-key JOIN should match alpha only");

        Spi::run(&format!(
            "DELETE FROM {} WHERE key = 'join_test:str:alpha';",
            table_name
        ))
        .unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE key = 'join_test:str:beta';",
            table_name
        ))
        .unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        let _ = Spi::run("DROP TABLE IF EXISTS join_str_local;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_large_dataset() {
        setup_join_fdw();

        let table_name = "join_large_hash";
        let key_prefix = "join_test:large_hash";
        create_join_table(table_name, "field text, value text", "hash", key_prefix);

        // Insert 100 rows to test moderate-scale join
        let mut insert_sql = String::from("INSERT INTO ");
        insert_sql.push_str(table_name);
        insert_sql.push_str(" VALUES ");
        for i in 0..100 {
            if i > 0 {
                insert_sql.push_str(", ");
            }
            insert_sql.push_str(&format!("('key{}', 'val{}')", i, i));
        }
        insert_sql.push(';');
        Spi::run(&insert_sql).unwrap();

        Spi::run("CREATE TEMPORARY TABLE join_large_local (id text);").unwrap();
        let mut local_sql = String::from("INSERT INTO join_large_local VALUES ");
        for i in 0..50 {
            if i > 0 {
                local_sql.push_str(", ");
            }
            local_sql.push_str(&format!("('key{}')", i * 2));
        }
        local_sql.push(';');
        Spi::run(&local_sql).unwrap();

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_large_local l JOIN {} r ON l.id = r.field;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(count, 50, "Should match 50 even-numbered keys");

        // Cleanup
        for i in 0..100 {
            Spi::run(&format!(
                "DELETE FROM {} WHERE field = 'key{}';",
                table_name, i
            ))
            .unwrap();
        }
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        let _ = Spi::run("DROP TABLE IF EXISTS join_large_local;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_rescan_correctness() {
        setup_join_fdw();

        let table_name = "join_rescan_hash";
        let key_prefix = "join_test:rescan_hash";
        create_join_table(table_name, "field text, value text", "hash", key_prefix);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('a', 'val_a'), ('b', 'val_b');",
            table_name
        ))
        .unwrap();

        // Subquery that forces multiple rescans of the FDW table
        Spi::run("CREATE TEMPORARY TABLE join_rescan_local (id text, grp int);").unwrap();
        Spi::run("INSERT INTO join_rescan_local VALUES ('a', 1), ('b', 1), ('a', 2), ('b', 2);")
            .unwrap();

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_rescan_local l JOIN {} r ON l.id = r.field;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            count, 4,
            "Rescan should correctly return results for duplicated local rows"
        );

        Spi::run(&format!("DELETE FROM {} WHERE field = 'a';", table_name)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'b';", table_name)).unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        let _ = Spi::run("DROP TABLE IF EXISTS join_rescan_local;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_right_join_fallback() {
        setup_join_fdw();

        let table_name = "join_right_hash";
        let key_prefix = "join_test:right_hash";
        create_join_table(table_name, "field text, value text", "hash", key_prefix);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('a', 'val_a'), ('b', 'val_b'), ('c', 'val_c');",
            table_name
        ))
        .unwrap();

        Spi::run("CREATE TEMPORARY TABLE join_right_local (id text, label text);").unwrap();
        Spi::run("INSERT INTO join_right_local VALUES ('a', 'L1'), ('b', 'L2'), ('d', 'L4');")
            .unwrap();

        // RIGHT JOIN: all local rows kept, Redis rows only where matched
        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM {} r RIGHT JOIN join_right_local l ON r.field = l.id;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            count, 3,
            "RIGHT JOIN should return all 3 local rows (a, b, d)"
        );

        // Verify unmatched Redis side produces NULL
        let null_field = Spi::get_one::<String>(&format!(
            "SELECT r.field FROM {} r RIGHT JOIN join_right_local l ON r.field = l.id WHERE l.id = 'd';",
            table_name
        ))
        .unwrap();

        assert!(
            null_field.is_none(),
            "RIGHT JOIN unmatched Redis row should be NULL, got: {:?}",
            null_field
        );

        Spi::run(&format!("DELETE FROM {} WHERE field = 'a';", table_name)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'b';", table_name)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'c';", table_name)).unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        let _ = Spi::run("DROP TABLE IF EXISTS join_right_local;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_cross_database_no_pushdown() {
        setup_join_fdw();

        // Create a second server pointing to a different database
        let server2 = "redis_join_server_db2";
        Spi::run(&format!(
            "CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (host_port '{}');",
            server2, FDW_NAME, REDIS_HOST_PORT
        ))
        .unwrap();

        let table1 = "join_crossdb_hash1";
        let table2 = "join_crossdb_hash2";
        let key1 = "join_test:crossdb_h1";
        let key2 = "join_test:crossdb_h2";

        // Table 1 on database 15 (default server)
        create_join_table(table1, "field text, value text", "hash", key1);

        // Table 2 on database 14 (different database via second server)
        Spi::run(&format!(
            "CREATE FOREIGN TABLE {} (field text, value text) SERVER {} OPTIONS (
                database '14',
                table_type 'hash',
                table_key_prefix '{}'
            );",
            table2, server2, key2
        ))
        .unwrap();

        Spi::run(&format!("INSERT INTO {} VALUES ('x', 'val_x');", table1)).unwrap();
        Spi::run(&format!("INSERT INTO {} VALUES ('x', 'val_x2');", table2)).unwrap();

        // JOIN across different databases — should still work via nested-loop
        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM {} t1 JOIN {} t2 ON t1.field = t2.field;",
            table1, table2
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            count, 1,
            "Cross-database JOIN should work via nested-loop fallback"
        );

        Spi::run(&format!("DELETE FROM {} WHERE field = 'x';", table1)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'x';", table2)).unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table1));
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table2));
        let _ = Spi::run(&format!("DROP SERVER IF EXISTS {} CASCADE;", server2));
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_fdw_to_fdw_left_join_null_handling() {
        setup_join_fdw();

        let hash_table = "join_fdw_null_hash";
        let set_table = "join_fdw_null_set";
        let hash_key = "join_test:fdw_null_hash";
        let set_key = "join_test:fdw_null_set";

        create_join_table(hash_table, "field text, value text", "hash", hash_key);
        create_join_table(set_table, "member text", "set", set_key);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('match', 'found'), ('only_hash', 'orphan');",
            hash_table
        ))
        .unwrap();
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('match'), ('only_set');",
            set_table
        ))
        .unwrap();

        // FDW-to-FDW LEFT JOIN: hash LEFT JOIN set ON hash.field = set.member
        // 'match' should produce a row; 'only_hash' should produce NULL for set.member
        let total = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM {} h LEFT JOIN {} s ON h.field = s.member;",
            hash_table, set_table
        ))
        .unwrap()
        .unwrap();

        assert_eq!(total, 2, "LEFT JOIN should return both hash rows");

        // Verify the unmatched row produces actual SQL NULL (not literal 'NULL')
        let null_member = Spi::get_one::<String>(&format!(
            "SELECT s.member FROM {} h LEFT JOIN {} s ON h.field = s.member WHERE h.field = 'only_hash';",
            hash_table, set_table
        ))
        .unwrap();

        assert!(
            null_member.is_none(),
            "Unmatched LEFT JOIN row should produce SQL NULL, got: {:?}",
            null_member
        );

        // Verify matched row returns correct value
        let matched = Spi::get_one::<String>(&format!(
            "SELECT s.member FROM {} h LEFT JOIN {} s ON h.field = s.member WHERE h.field = 'match';",
            hash_table, set_table
        ))
        .unwrap()
        .unwrap();

        assert_eq!(matched, "match", "Matched row should return correct member");

        Spi::run(&format!(
            "DELETE FROM {} WHERE field = 'match';",
            hash_table
        ))
        .unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE field = 'only_hash';",
            hash_table
        ))
        .unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE member = 'match';",
            set_table
        ))
        .unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE member = 'only_set';",
            set_table
        ))
        .unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", hash_table));
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", set_table));
        cleanup_join_fdw();
    }
}
