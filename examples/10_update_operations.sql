-- ============================================================
-- Demo 10: UPDATE Operations
-- ============================================================
-- The FDW maps UPDATE to the appropriate Redis command:
--   String: SET (full value replace)
--   Hash:   HSET field (single-field update)
--   ZSet:   ZADD member score (score update)
-- This file focuses on what UPDATE does, with EXPLAIN to show
-- pushdown behavior.
-- ============================================================

\timing on

-- ------------------------------------------------------------
-- Part A: UPDATE on STRING (full-value replace via SET)
-- ------------------------------------------------------------
CREATE FOREIGN TABLE demo_upd_string (value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'string',
        table_key_prefix 'upd:config:version'
    );

INSERT INTO demo_upd_string VALUES ('1.0.0');
SELECT * FROM demo_upd_string;

EXPLAIN (VERBOSE) UPDATE demo_upd_string SET value = '2.0.0';
UPDATE demo_upd_string SET value = '2.0.0';
SELECT * FROM demo_upd_string;

-- ------------------------------------------------------------
-- Part B: UPDATE on HASH (single field via HSET)
-- ------------------------------------------------------------
CREATE FOREIGN TABLE demo_upd_hash (field text, value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'hash',
        table_key_prefix 'upd:user:profile'
    );

INSERT INTO demo_upd_hash VALUES
    ('name', 'Alice'),
    ('role', 'engineer'),
    ('city', 'Taipei');

SELECT * FROM demo_upd_hash;

-- Update one field — pushdown uses HSET (not HGETALL + rewrite)
UPDATE demo_upd_hash SET value = 'staff engineer' WHERE field = 'role';
SELECT * FROM demo_upd_hash;

-- UPDATE without WHERE: every field gets the same value (rarely useful)
UPDATE demo_upd_hash SET value = 'redacted' WHERE field IN ('name', 'city');
SELECT * FROM demo_upd_hash;

-- ------------------------------------------------------------
-- Part C: UPDATE on ZSET (score change via ZADD)
-- ------------------------------------------------------------
CREATE FOREIGN TABLE demo_upd_zset (member text, score numeric)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'zset',
        table_key_prefix 'upd:game:leaderboard'
    );

INSERT INTO demo_upd_zset VALUES
    ('alice', 1000),
    ('bob', 1500),
    ('charlie', 800);

SELECT * FROM demo_upd_zset ORDER BY score DESC;

-- Bump alice's score
EXPLAIN (VERBOSE) UPDATE demo_upd_zset SET score = score + 500 WHERE member = 'alice';
UPDATE demo_upd_zset SET score = 1500 WHERE member = 'alice';
SELECT * FROM demo_upd_zset ORDER BY score DESC;

-- ------------------------------------------------------------
-- Cleanup
-- ------------------------------------------------------------
DROP FOREIGN TABLE demo_upd_string;
DROP FOREIGN TABLE demo_upd_hash;
DROP FOREIGN TABLE demo_upd_zset;
