-- ============================================================
-- Demo 13: TRUNCATE
-- ============================================================
-- TRUNCATE on a Redis foreign table removes the underlying key(s):
--   Single-key table:  DEL / UNLINK on the one key
--   Multi-key pattern: SCAN + UNLINK for every matching key
-- ============================================================

\timing on

-- ------------------------------------------------------------
-- Part A: Single-key TRUNCATE
-- ------------------------------------------------------------
CREATE FOREIGN TABLE demo_trunc_hash (field text, value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'hash',
        table_key_prefix 'trunc:test:hash'
    );

INSERT INTO demo_trunc_hash VALUES ('a', '1'), ('b', '2'), ('c', '3');
SELECT * FROM demo_trunc_hash;

TRUNCATE demo_trunc_hash;
SELECT * FROM demo_trunc_hash;  -- empty

-- ------------------------------------------------------------
-- Part B: Multi-key pattern TRUNCATE (SCAN + UNLINK)
-- ------------------------------------------------------------
CREATE FOREIGN TABLE demo_trunc_seed1 (value text)
    SERVER redis_server
    OPTIONS (database '0', table_type 'string', table_key_prefix 'trunc:multi:1');
CREATE FOREIGN TABLE demo_trunc_seed2 (value text)
    SERVER redis_server
    OPTIONS (database '0', table_type 'string', table_key_prefix 'trunc:multi:2');
CREATE FOREIGN TABLE demo_trunc_seed3 (value text)
    SERVER redis_server
    OPTIONS (database '0', table_type 'string', table_key_prefix 'trunc:multi:3');

INSERT INTO demo_trunc_seed1 VALUES ('a');
INSERT INTO demo_trunc_seed2 VALUES ('b');
INSERT INTO demo_trunc_seed3 VALUES ('c');

CREATE FOREIGN TABLE demo_trunc_pattern (key text, value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'string',
        table_key_prefix 'trunc:multi:*'
    );

SELECT * FROM demo_trunc_pattern;  -- 3 rows
TRUNCATE demo_trunc_pattern;       -- removes all matching keys
SELECT * FROM demo_trunc_pattern;  -- empty

-- ------------------------------------------------------------
-- Cleanup
-- ------------------------------------------------------------
DROP FOREIGN TABLE demo_trunc_hash;
DROP FOREIGN TABLE demo_trunc_seed1;
DROP FOREIGN TABLE demo_trunc_seed2;
DROP FOREIGN TABLE demo_trunc_seed3;
DROP FOREIGN TABLE demo_trunc_pattern;
