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

    #[pg_test]
    fn test_join_list_duplicate_elements() {
        setup_join_fdw();

        let table_name = "join_list_dup";
        let key_prefix = "join_test:list_dup";
        create_join_table(table_name, "element text", "list", key_prefix);

        // List allows duplicate elements
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('dup'), ('dup'), ('unique'), ('dup');",
            table_name
        ))
        .unwrap();

        Spi::run("CREATE TEMPORARY TABLE join_dup_local (val text);").unwrap();
        Spi::run("INSERT INTO join_dup_local VALUES ('dup'), ('unique'), ('missing');").unwrap();

        // INNER JOIN: 'dup' in local matches 3 list elements, 'unique' matches 1 = total 4
        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_dup_local l JOIN {} r ON l.val = r.element;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            count, 4,
            "Duplicate list elements should produce cross-product: 3 + 1 = 4 rows"
        );

        // LEFT JOIN: all 3 local rows, 'missing' gets NULL
        let left_count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_dup_local l LEFT JOIN {} r ON l.val = r.element;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            left_count, 5,
            "LEFT JOIN with duplicates: 3 (dup) + 1 (unique) + 1 (missing/NULL) = 5"
        );

        Spi::run(&format!(
            "DELETE FROM {} WHERE element = 'dup';",
            table_name
        ))
        .unwrap();
        Spi::run(&format!(
            "DELETE FROM {} WHERE element = 'unique';",
            table_name
        ))
        .unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        let _ = Spi::run("DROP TABLE IF EXISTS join_dup_local;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_fdw_left_join_inner_larger_than_outer() {
        setup_join_fdw();

        // Outer (left) table: small (3 rows)
        let small_table = "join_fdw_small_hash";
        let small_key = "join_test:small_hash";
        create_join_table(small_table, "field text, value text", "hash", small_key);

        // Inner (right) table: large (10 rows)
        let large_table = "join_fdw_large_hash";
        let large_key = "join_test:large_hash2";
        create_join_table(large_table, "field text, value text", "hash", large_key);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('a', 'small_a'), ('b', 'small_b'), ('c', 'small_c');",
            small_table
        ))
        .unwrap();

        let mut insert = format!("INSERT INTO {} VALUES ", large_table);
        for i in 0..10 {
            if i > 0 {
                insert.push_str(", ");
            }
            let key = (b'a' + (i as u8)) as char;
            insert.push_str(&format!("('{}', 'large_{}')", key, key));
        }
        insert.push(';');
        Spi::run(&insert).unwrap();

        // LEFT JOIN: outer=small(3), inner=large(10)
        // Build side should be small (3 rows), probe side should be large (10 rows)
        // All 3 small rows should appear; a, b, c all match in large
        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM {} s LEFT JOIN {} l ON s.field = l.field;",
            small_table, large_table
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            count, 3,
            "LEFT JOIN: all 3 outer rows should be present (all match)"
        );

        // Verify data correctness
        let val = Spi::get_one::<String>(&format!(
            "SELECT l.value FROM {} s LEFT JOIN {} l ON s.field = l.field WHERE s.field = 'a';",
            small_table, large_table
        ))
        .unwrap()
        .unwrap();

        assert_eq!(val, "large_a", "Should get the matching inner value");

        // Now test with a key that only exists in small (add one)
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('zzz', 'only_small');",
            small_table
        ))
        .unwrap();

        let left_null = Spi::get_one::<String>(&format!(
            "SELECT l.value FROM {} s LEFT JOIN {} l ON s.field = l.field WHERE s.field = 'zzz';",
            small_table, large_table
        ))
        .unwrap();

        assert!(
            left_null.is_none(),
            "LEFT JOIN unmatched outer row should produce NULL inner, got: {:?}",
            left_null
        );

        // Cleanup
        for c in 'a'..='c' {
            Spi::run(&format!(
                "DELETE FROM {} WHERE field = '{}';",
                small_table, c
            ))
            .unwrap();
        }
        Spi::run(&format!("DELETE FROM {} WHERE field = 'zzz';", small_table)).unwrap();
        for i in 0..10 {
            let key = (b'a' + (i as u8)) as char;
            Spi::run(&format!(
                "DELETE FROM {} WHERE field = '{}';",
                large_table, key
            ))
            .unwrap();
        }
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", small_table));
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", large_table));
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_self_join_same_table() {
        setup_join_fdw();

        let table_name = "join_self_hash";
        let key_prefix = "join_test:self_hash";
        create_join_table(table_name, "field text, value text", "hash", key_prefix);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('a', 'a'), ('b', 'c'), ('c', 'x');",
            table_name
        ))
        .unwrap();

        // Self-join: field of one alias matches value of another
        // t1.field = t2.value: 'a'='a' (t1.a matches t2 where value='a' → t2.field='a')
        //                      'c'='c' (t1.c doesn't exist as field... wait)
        // Let's do simpler: join on same column (field = field)
        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM {} t1 JOIN {} t2 ON t1.field = t2.field;",
            table_name, table_name
        ))
        .unwrap()
        .unwrap();

        // Self-join on field=field should match all 3 rows (each matches itself)
        assert_eq!(count, 3, "Self-join on same column should match all rows");

        // Self-join on t1.field = t2.value
        // t2 has values: 'a', 'c', 'x'
        // t1 fields: 'a', 'b', 'c'
        // Matches: t1.field='a' = t2.value='a' (from t2 row ('a','a'))
        //          t1.field='c' = t2.value='c' (from t2 row ('b','c'))
        let cross_count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM {} t1 JOIN {} t2 ON t1.field = t2.value;",
            table_name, table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            cross_count, 2,
            "Self-join field=value should match 'a' and 'c'"
        );

        Spi::run(&format!("DELETE FROM {} WHERE field = 'a';", table_name)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'b';", table_name)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'c';", table_name)).unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_fdw_inner_join_no_matches() {
        setup_join_fdw();

        let table1 = "join_nomatch_hash1";
        let table2 = "join_nomatch_hash2";
        let key1 = "join_test:nomatch_h1";
        let key2 = "join_test:nomatch_h2";

        create_join_table(table1, "field text, value text", "hash", key1);
        create_join_table(table2, "field text, value text", "hash", key2);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('a', 'val_a'), ('b', 'val_b');",
            table1
        ))
        .unwrap();
        Spi::run(&format!(
            "INSERT INTO {} VALUES ('x', 'val_x'), ('y', 'val_y');",
            table2
        ))
        .unwrap();

        // INNER JOIN with no matching keys should return 0 rows
        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM {} t1 JOIN {} t2 ON t1.field = t2.field;",
            table1, table2
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            count, 0,
            "INNER JOIN with no matching keys should return 0 rows"
        );

        Spi::run(&format!("DELETE FROM {} WHERE field = 'a';", table1)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'b';", table1)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'x';", table2)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'y';", table2)).unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table1));
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table2));
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_fdw_to_fdw_build_probe_strategy() {
        setup_join_fdw();

        // Create a small (2 rows) and large (20 rows) table
        // The optimizer should build on the smaller side regardless of SQL order
        let small_table = "join_bp_small";
        let large_table = "join_bp_large";
        let small_key = "join_test:bp_small";
        let large_key = "join_test:bp_large";

        create_join_table(small_table, "field text, value text", "hash", small_key);
        create_join_table(large_table, "field text, value text", "hash", large_key);

        Spi::run(&format!(
            "INSERT INTO {} VALUES ('k1', 'small1'), ('k2', 'small2');",
            small_table
        ))
        .unwrap();

        let mut insert = format!("INSERT INTO {} VALUES ", large_table);
        for i in 1..=20 {
            if i > 1 {
                insert.push_str(", ");
            }
            insert.push_str(&format!("('k{}', 'large{}')", i, i));
        }
        insert.push(';');
        Spi::run(&insert).unwrap();

        // Join with large first in FROM — should still get correct results
        let count1 = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM {} l JOIN {} s ON l.field = s.field;",
            large_table, small_table
        ))
        .unwrap()
        .unwrap();

        assert_eq!(count1, 2, "Large JOIN Small should find 2 matches (k1, k2)");

        // Join with small first in FROM — same result
        let count2 = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM {} s JOIN {} l ON s.field = l.field;",
            small_table, large_table
        ))
        .unwrap()
        .unwrap();

        assert_eq!(count2, 2, "Small JOIN Large should find 2 matches (k1, k2)");

        // Verify content regardless of build/probe side
        let val = Spi::get_one::<String>(&format!(
            "SELECT s.value FROM {} l JOIN {} s ON l.field = s.field WHERE l.field = 'k1';",
            large_table, small_table
        ))
        .unwrap()
        .unwrap();

        assert_eq!(val, "small1", "Should get correct value from small table");

        // Cleanup
        Spi::run(&format!("DELETE FROM {} WHERE field = 'k1';", small_table)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'k2';", small_table)).unwrap();
        for i in 1..=20 {
            Spi::run(&format!(
                "DELETE FROM {} WHERE field = 'k{}';",
                large_table, i
            ))
            .unwrap();
        }
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", small_table));
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", large_table));
        cleanup_join_fdw();
    }

    // === TTL-position-aware join tests ===

    #[pg_test]
    fn test_join_hash_ttl_first_with_local() {
        setup_join_fdw();

        let key_prefix = "join_test:ttl_first_hash";
        let table_name = "join_ttl_first_hash";

        // TTL column at position 0, before data columns
        Spi::run(&format!(
            "CREATE FOREIGN TABLE {} (ttl bigint, field text, value text)
             SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix '{}', ttl '3600'
             );",
            table_name, SERVER_NAME, TEST_DATABASE, key_prefix
        ))
        .unwrap();

        Spi::run(&format!(
            "INSERT INTO {} (field, value) VALUES ('a', 'val_a'), ('b', 'val_b'), ('c', 'val_c');",
            table_name
        ))
        .unwrap();

        Spi::run("CREATE TEMPORARY TABLE join_ttl_local (id text, name text);").unwrap();
        Spi::run("INSERT INTO join_ttl_local VALUES ('a', 'Alice'), ('b', 'Bob'), ('d', 'Dave');")
            .unwrap();

        // Parameterized HGET path should work even with TTL at position 0
        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_ttl_local l JOIN {} r ON l.id = r.field;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            count, 2,
            "JOIN with TTL-first hash should find 2 matches (a, b)"
        );

        // LEFT JOIN
        let left_count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_ttl_local l LEFT JOIN {} r ON l.id = r.field;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            left_count, 3,
            "LEFT JOIN with TTL-first should return all 3 local rows"
        );

        Spi::run(&format!("DELETE FROM {} WHERE field = 'a';", table_name)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'b';", table_name)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'c';", table_name)).unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        let _ = Spi::run("DROP TABLE IF EXISTS join_ttl_local;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_hash_ttl_middle_with_local() {
        setup_join_fdw();

        let key_prefix = "join_test:ttl_mid_hash";
        let table_name = "join_ttl_mid_hash";

        // TTL column between data columns
        Spi::run(&format!(
            "CREATE FOREIGN TABLE {} (field text, ttl bigint, value text)
             SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix '{}', ttl '3600'
             );",
            table_name, SERVER_NAME, TEST_DATABASE, key_prefix
        ))
        .unwrap();

        Spi::run(&format!(
            "INSERT INTO {} (field, value) VALUES ('x', 'val_x'), ('y', 'val_y');",
            table_name
        ))
        .unwrap();

        Spi::run("CREATE TEMPORARY TABLE join_ttl_mid_local (id text);").unwrap();
        Spi::run("INSERT INTO join_ttl_mid_local VALUES ('x'), ('y'), ('z');").unwrap();

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_ttl_mid_local l JOIN {} r ON l.id = r.field;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(count, 2, "JOIN with TTL-middle hash should find 2 matches");

        Spi::run(&format!("DELETE FROM {} WHERE field = 'x';", table_name)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'y';", table_name)).unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        let _ = Spi::run("DROP TABLE IF EXISTS join_ttl_mid_local;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_zset_ttl_first_with_local() {
        setup_join_fdw();

        let key_prefix = "join_test:ttl_first_zset";
        let table_name = "join_ttl_first_zset";

        // TTL at position 0 for zset
        Spi::run(&format!(
            "CREATE FOREIGN TABLE {} (ttl bigint, member text, score text)
             SERVER {} OPTIONS (
                database '{}', table_type 'zset', table_key_prefix '{}', ttl '3600'
             );",
            table_name, SERVER_NAME, TEST_DATABASE, key_prefix
        ))
        .unwrap();

        Spi::run(&format!(
            "INSERT INTO {} (member, score) VALUES ('p1', 10), ('p2', 20), ('p3', 30);",
            table_name
        ))
        .unwrap();

        Spi::run("CREATE TEMPORARY TABLE join_ttl_zset_local (name text);").unwrap();
        Spi::run("INSERT INTO join_ttl_zset_local VALUES ('p1'), ('p2'), ('missing');").unwrap();

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_ttl_zset_local l JOIN {} r ON l.name = r.member;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(count, 2, "JOIN with TTL-first zset should match p1 and p2");

        Spi::run(&format!("DELETE FROM {} WHERE member = 'p1';", table_name)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE member = 'p2';", table_name)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE member = 'p3';", table_name)).unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        let _ = Spi::run("DROP TABLE IF EXISTS join_ttl_zset_local;");
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_fdw_hash_to_hash_ttl_first() {
        setup_join_fdw();

        let table1 = "join_ttl_fdw_h1";
        let table2 = "join_ttl_fdw_h2";
        let key1 = "join_test:ttl_fdw_h1";
        let key2 = "join_test:ttl_fdw_h2";

        // Both tables with TTL at position 0
        Spi::run(&format!(
            "CREATE FOREIGN TABLE {} (ttl bigint, field text, value text)
             SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix '{}', ttl '3600'
             );",
            table1, SERVER_NAME, TEST_DATABASE, key1
        ))
        .unwrap();

        Spi::run(&format!(
            "CREATE FOREIGN TABLE {} (ttl bigint, field text, value text)
             SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix '{}', ttl '3600'
             );",
            table2, SERVER_NAME, TEST_DATABASE, key2
        ))
        .unwrap();

        Spi::run(&format!(
            "INSERT INTO {} (field, value) VALUES ('k1', 'v1a'), ('k2', 'v2a');",
            table1
        ))
        .unwrap();
        Spi::run(&format!(
            "INSERT INTO {} (field, value) VALUES ('k1', 'v1b'), ('k3', 'v3b');",
            table2
        ))
        .unwrap();

        // FDW-to-FDW join with TTL at position 0 on both sides
        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM {} t1 JOIN {} t2 ON t1.field = t2.field;",
            table1, table2
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            count, 1,
            "FDW-to-FDW with TTL-first should find 1 match (k1)"
        );

        // LEFT JOIN
        let left_count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM {} t1 LEFT JOIN {} t2 ON t1.field = t2.field;",
            table1, table2
        ))
        .unwrap()
        .unwrap();

        assert_eq!(left_count, 2, "LEFT JOIN should return both rows from t1");

        // Verify content
        let val = Spi::get_one::<String>(&format!(
            "SELECT t2.value FROM {} t1 JOIN {} t2 ON t1.field = t2.field WHERE t1.field = 'k1';",
            table1, table2
        ))
        .unwrap()
        .unwrap();

        assert_eq!(
            val, "v1b",
            "Should get correct value from t2 via TTL-first join"
        );

        Spi::run(&format!("DELETE FROM {} WHERE field = 'k1';", table1)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'k2';", table1)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'k1';", table2)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE field = 'k3';", table2)).unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table1));
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table2));
        cleanup_join_fdw();
    }

    #[pg_test]
    fn test_join_set_ttl_first_with_local() {
        setup_join_fdw();

        let key_prefix = "join_test:ttl_first_set";
        let table_name = "join_ttl_first_set";

        // Set with TTL at position 0
        Spi::run(&format!(
            "CREATE FOREIGN TABLE {} (ttl bigint, member text)
             SERVER {} OPTIONS (
                database '{}', table_type 'set', table_key_prefix '{}', ttl '3600'
             );",
            table_name, SERVER_NAME, TEST_DATABASE, key_prefix
        ))
        .unwrap();

        Spi::run(&format!(
            "INSERT INTO {} (member) VALUES ('m1'), ('m2'), ('m3');",
            table_name
        ))
        .unwrap();

        Spi::run("CREATE TEMPORARY TABLE join_ttl_set_local (id text);").unwrap();
        Spi::run("INSERT INTO join_ttl_set_local VALUES ('m1'), ('m2'), ('missing');").unwrap();

        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM join_ttl_set_local l JOIN {} r ON l.id = r.member;",
            table_name
        ))
        .unwrap()
        .unwrap();

        assert_eq!(count, 2, "SET JOIN with TTL-first should match m1 and m2");

        Spi::run(&format!("DELETE FROM {} WHERE member = 'm1';", table_name)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE member = 'm2';", table_name)).unwrap();
        Spi::run(&format!("DELETE FROM {} WHERE member = 'm3';", table_name)).unwrap();
        let _ = Spi::run(&format!("DROP FOREIGN TABLE IF EXISTS {};", table_name));
        let _ = Spi::run("DROP TABLE IF EXISTS join_ttl_set_local;");
        cleanup_join_fdw();
    }
}
