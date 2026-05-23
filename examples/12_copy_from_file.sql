-- ============================================================
-- Demo 12: COPY FROM (file-based bulk load)
-- ============================================================
-- COPY FROM uses the BeginForeignInsert / EndForeignInsert path
-- which lets the FDW pipeline rows in batches.
--
-- This file:
--   1. Generates a 10K-row CSV server-side using COPY ... TO PROGRAM
--      so the demo is fully self-contained (no manual file prep).
--   2. Loads it back into a Redis hash via COPY FROM.
--
-- If your environment forbids COPY ... TO PROGRAM, replace it with
-- a pre-generated CSV path. The COPY FROM portion is the actual
-- feature being demonstrated.
-- ============================================================

\timing on

-- ------------------------------------------------------------
-- Step 1: Generate a 10K-row CSV file via a local staging table
-- ------------------------------------------------------------
CREATE TEMPORARY TABLE staging_events (field text, value text);

INSERT INTO staging_events
SELECT
    'evt:' || g::text,
    '{"type":"click","page":"/p/' || (g % 50) || '","ts":' || (1700000000 + g) || '}'
FROM generate_series(1, 10000) g;

-- Write to /tmp/redis_fdw_events.csv (adjust path for Windows demos)
COPY staging_events TO '/tmp/redis_fdw_events.csv' WITH (FORMAT csv);

-- ------------------------------------------------------------
-- Step 2: COPY FROM file into a Redis foreign table
-- ------------------------------------------------------------
CREATE FOREIGN TABLE copy_events (field text, value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'hash',
        table_key_prefix 'copy:events:log',
        batch_size '5000'
    );

COPY copy_events FROM '/tmp/redis_fdw_events.csv' WITH (FORMAT csv);

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

-- ------------------------------------------------------------
-- Cleanup
-- ------------------------------------------------------------
TRUNCATE copy_events;
TRUNCATE copy_stdin_demo;
DROP FOREIGN TABLE copy_events;
DROP FOREIGN TABLE copy_stdin_demo;
DROP TABLE staging_events;
