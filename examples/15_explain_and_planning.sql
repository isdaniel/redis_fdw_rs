-- ============================================================
-- Demo 15: EXPLAIN Output & Query Planning
-- ============================================================
-- The FDW provides detailed EXPLAIN output showing:
--   - Redis server connection details
--   - Key prefix and table type
--   - Pushdown conditions applied
--   - Batch size for bulk operations
-- ============================================================

CREATE FOREIGN TABLE demo_explain_hash (field text, value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'hash',
        table_key_prefix 'explain:demo:hash'
    );

INSERT INTO demo_explain_hash VALUES ('a', '1'), ('b', '2'), ('c', '3');

-- Basic EXPLAIN shows the Foreign Scan node
EXPLAIN SELECT * FROM demo_explain_hash;

-- VERBOSE shows server, key, type, and pushdown details
EXPLAIN (VERBOSE) SELECT * FROM demo_explain_hash;

-- With pushdown condition
EXPLAIN (VERBOSE) SELECT value FROM demo_explain_hash WHERE field = 'a';

-- EXPLAIN ANALYZE shows actual execution time
EXPLAIN (ANALYZE, VERBOSE) SELECT * FROM demo_explain_hash;

-- EXPLAIN for modification operations
EXPLAIN (VERBOSE) INSERT INTO demo_explain_hash VALUES ('d', '4');
EXPLAIN (VERBOSE) UPDATE demo_explain_hash SET value = '99' WHERE field = 'a';
EXPLAIN (VERBOSE) DELETE FROM demo_explain_hash WHERE field = 'b';

-- Cleanup
DROP FOREIGN TABLE demo_explain_hash;
