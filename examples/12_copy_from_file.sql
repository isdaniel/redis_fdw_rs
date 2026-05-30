-- ============================================================
-- Demo 12: COPY FROM (bulk load into Redis)
-- ============================================================
-- COPY FROM uses the BeginForeignInsert / EndForeignInsert path
-- which lets the FDW pipeline rows in batches.
--
-- This file demonstrates:
--   1. INSERT ... SELECT from a local table into a Redis hash (uses
--      the same BeginForeignInsert / batch pipeline path as COPY FROM)
--   2. COPY FROM stdin (inline data, no file needed)
-- ============================================================

\timing on

-- ------------------------------------------------------------
-- Step 1: Generate 10K rows in a staging table, bulk-load into Redis
-- ------------------------------------------------------------
CREATE TEMPORARY TABLE staging_events (field text, value text);

INSERT INTO staging_events
SELECT
    'evt:' || g::text,
    '{"type":"click","page":"/p/' || (g % 50) || '","ts":' || (1700000000 + g) || '}'
FROM generate_series(1, 10000) g;

CREATE FOREIGN TABLE copy_events (field text, value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'hash',
        table_key_prefix 'copy:events:log',
        batch_size '5000'
    );

-- Bulk-load via INSERT ... SELECT (same pipeline as COPY FROM)
INSERT INTO copy_events SELECT * FROM staging_events;

-- ------------------------------------------------------------
-- Step 3: Verify
-- ------------------------------------------------------------
SELECT COUNT(*) AS events_loaded FROM copy_events;
SELECT * FROM copy_events WHERE field = 'evt:1';
SELECT * FROM copy_events WHERE field = 'evt:5000';

-- ------------------------------------------------------------
-- Bonus: COPY FROM stdin (inline data, no file needed)
-- ------------------------------------------------------------
CREATE FOREIGN TABLE copy_stdin_demo (field text, value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'hash',
        table_key_prefix 'copy:stdin:demo'
    );

COPY copy_stdin_demo (field, value) FROM stdin WITH (FORMAT csv);
sku:1001,"{""name"":""keyboard"",""price"":129.99}"
sku:1002,"{""name"":""mouse"",""price"":49.99}"
sku:1003,"{""name"":""monitor"",""price"":399.99}"
\.

SELECT * FROM copy_stdin_demo;

-- -- ------------------------------------------------------------
-- -- Target table: Redis HASH backed by a single key
-- -- batch_size = 10000 → ~10 pipelined Redis round-trips for 100K rows
-- -- ------------------------------------------------------------
-- CREATE FOREIGN TABLE copy_users_100k (field text, value text)
--     SERVER redis_server
--     OPTIONS (
--         database '0',
--         table_type 'hash',
--         table_key_prefix 'copy:users:100k',
--         batch_size '10000'
--     );



-- -- ------------------------------------------------------------
-- -- Bulk load via \copy (client-side; no superuser required)
-- -- Run psql from the examples/ directory, or adjust the path.
-- -- ------------------------------------------------------------
-- \copy copy_users_100k (field, value) FROM 'users_100k.csv' WITH (FORMAT csv);

-- -- redis-cli HGETALL copy:users:100k
-- -- redis-cli HGET copy:users:100k user:50000
-- -- redis-cli HMGET copy:users:100k user:1 user:50000 user:100000

-- EXPLAIN (ANALYZE, VERBOSE) 
-- SELECT COUNT(*) AS rows_loaded FROM copy_users_100k;

-- EXPLAIN (ANALYZE, VERBOSE) 
-- SELECT * FROM copy_users_100k where field in ('user:1', 'user:50000', 'user:100000');

-- ------------------------------------------------------------
-- Cleanup
-- ------------------------------------------------------------
TRUNCATE copy_events;
TRUNCATE copy_stdin_demo;
DROP FOREIGN TABLE copy_events;
DROP FOREIGN TABLE copy_stdin_demo;
-- DROP FOREIGN TABLE copy_users_100k;
DROP TABLE staging_events;
