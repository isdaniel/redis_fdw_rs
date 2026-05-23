-- ============================================================
-- Demo 11: Bulk Insert (100K) + Pushdown-Driven Large JOIN
-- ============================================================
-- The headliner perf demo. Two features, one dataset:
--
--   Part 1: Bulk-load 100K rows (batch_size A/B test).
--   Part 2: Pushdown JOIN — normal PG table ↔ Redis FDW.
--             Shows the default full-scan plan vs the
--             parameterized nested-loop plan that pushes
--             one HGET per outer row.
--   Part 3: Pushdown JOIN — Redis FDW ↔ Redis FDW.
--             SET membership + HASH lookup, both pushed.
--   Part 4: Bonus — ZSet score-range pushdown at scale.
--
-- The trick: PG only pushes "field = $1" when the planner picks
-- a Nested Loop with a parameterized inner-side Foreign Scan.
-- We turn off hash/merge join briefly to force that shape so the
-- pushdown is visible on EXPLAIN.
-- ============================================================

\timing on
\set ROWS 100000

-- ============================================================
-- PART 1: BULK INSERT 100K rows (batch_size A/B)
-- ============================================================

-- 1A. Baseline - small batch_size (more round-trips, slower)
CREATE FOREIGN TABLE bulk_user_profiles_slow (field text, value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'hash',
        table_key_prefix 'demo11:users:slow',
        batch_size '500'
    );

INSERT INTO bulk_user_profiles_slow
SELECT
    'user:' || g::text,
    '{"name":"u_' || g::text || '","score":' || (random() * 1000)::int::text || '}'
FROM generate_series(1, :ROWS) g;

SELECT COUNT(*) AS loaded_slow FROM bulk_user_profiles_slow;

-- 1B. Tuned - large batch_size (fewer round-trips, faster)
--     This is the table the JOIN demos run against.
CREATE FOREIGN TABLE bulk_user_profiles (field text, value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'hash',
        table_key_prefix 'demo11:users:profiles',
        batch_size '10000'
    );

INSERT INTO bulk_user_profiles
SELECT
    'user:' || g::text,
    '{"name":"u_' || g::text || '","score":' || (random() * 1000)::int::text || '}'
FROM generate_series(1, :ROWS) g;

SELECT COUNT(*) AS loaded_fast FROM bulk_user_profiles;

-- Companion: active users (Redis SET, 50K subset)
CREATE FOREIGN TABLE bulk_active_users (member text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'set',
        table_key_prefix 'demo11:users:active',
        batch_size '10000'
    );

INSERT INTO bulk_active_users
SELECT 'user:' || g::text
FROM generate_series(1, 50000) g;

SELECT COUNT(*) AS active_users FROM bulk_active_users;

-- ============================================================
-- PART 2: Pushdown JOIN — local PG table × Redis FDW
-- ============================================================
-- Strategy: small local driver table (200 rows). When the planner
-- picks a Nested Loop, the FDW gets `field = $1` per outer row
-- and turns each into a single HGET — instead of one HGETALL
-- fetching all 100K rows.

-- A small "VIP" list — 200 specific users we want to enrich
CREATE TEMPORARY TABLE vip_users (user_id text PRIMARY KEY);
INSERT INTO vip_users
SELECT 'user:' || g::text
FROM generate_series(1, 200) g;

-- ---- 2A. Default plan (planner picks Hash Join → full FDW scan) ----
-- Foreign Scan returns all 100K rows via HGETALL.
EXPLAIN (ANALYZE, VERBOSE)
SELECT v.user_id, p.value AS profile
FROM vip_users v
JOIN bulk_user_profiles p ON v.user_id = p.field;

-- ---- 2B. Force pushdown plan via Nested Loop ----
-- Disable hash/merge join so PG picks Nested Loop with
-- parameterized Foreign Scan. EXPLAIN should now show:
--   Foreign Scan on bulk_user_profiles
--     Filter: (field = v.user_id)      ← pushed to HGET
SET LOCAL enable_hashjoin = off;
SET LOCAL enable_mergejoin = off;

EXPLAIN (ANALYZE, VERBOSE)
SELECT v.user_id, p.value AS profile
FROM vip_users v
JOIN bulk_user_profiles p ON v.user_id = p.field;

-- Same shape — aggregation over the pushdown plan
EXPLAIN (ANALYZE, VERBOSE)
SELECT
    v.user_id,
    p.value AS profile
FROM vip_users v
JOIN bulk_user_profiles p ON v.user_id = p.field
ORDER BY v.user_id
LIMIT 10;

RESET enable_hashjoin;
RESET enable_mergejoin;

-- ============================================================
-- PART 3: Pushdown JOIN — Redis FDW × Redis FDW
-- ============================================================
-- Driver is the SET membership lookup. We pick a small slice
-- (50 active users via IN list) so the outer side is tiny and
-- the inner HASH lookups become per-row HGETs.

-- ---- 3A. SET pushdown: WHERE member IN (...) → SISMEMBER × N ----
EXPLAIN (ANALYZE, VERBOSE)
SELECT a.member
FROM bulk_active_users a
WHERE a.member IN (
    'user:1','user:42','user:100','user:777','user:9999',
    'user:12345','user:55555','user:88888','user:99999','user:99000'
);

-- ---- 3B. Two-sided pushdown: small SET filter feeds HASH HGETs ----
-- Outer: SET filtered by IN list (pushed to SISMEMBER per item).
-- Inner: HASH looked up by field = a.member (pushed to HGET).
SET LOCAL enable_hashjoin = off;
SET LOCAL enable_mergejoin = off;

EXPLAIN (ANALYZE, VERBOSE)
SELECT a.member AS user_id, p.value AS profile
FROM bulk_active_users a
JOIN bulk_user_profiles p ON p.field = a.member
WHERE a.member IN (
    'user:1','user:42','user:100','user:777','user:9999',
    'user:12345','user:55555','user:88888','user:99999','user:99000'
);

RESET enable_hashjoin;
RESET enable_mergejoin;

-- ---- 3C. Contrast: same JOIN without the selective filter ----
-- Both sides scanned fully (HGETALL + SMEMBERS) and hash-joined
-- in PG. Useful to show the default plan when the driver is large.
EXPLAIN (ANALYZE, VERBOSE)
SELECT COUNT(*) AS active_with_profile
FROM bulk_user_profiles p
JOIN bulk_active_users a ON p.field = a.member;

-- ============================================================
-- PART 4: ZSet score-range pushdown at scale
-- ============================================================
-- 100K leaderboard entries; selective range becomes ZRANGEBYSCORE
-- and fetches ~1% of the data instead of all 100K.

CREATE FOREIGN TABLE bulk_leaderboard (member text, score numeric)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'zset',
        table_key_prefix 'demo11:leaderboard',
        batch_size '10000'
    );

INSERT INTO bulk_leaderboard
SELECT 'player:' || g::text, (random() * 100000)::int
FROM generate_series(1, :ROWS) g;

-- Full scan baseline
EXPLAIN (ANALYZE, VERBOSE)
SELECT COUNT(*) FROM bulk_leaderboard;

-- Pushed score range — ZRANGEBYSCORE, fetches only top ~1%
EXPLAIN (ANALYZE, VERBOSE)
SELECT * FROM bulk_leaderboard
WHERE score >= 99000
ORDER BY score DESC;

-- ============================================================
-- Cleanup
-- ============================================================
TRUNCATE bulk_user_profiles_slow;
TRUNCATE bulk_user_profiles;
TRUNCATE bulk_active_users;
TRUNCATE bulk_leaderboard;
DROP FOREIGN TABLE bulk_user_profiles_slow;
DROP FOREIGN TABLE bulk_user_profiles;
DROP FOREIGN TABLE bulk_active_users;
DROP FOREIGN TABLE bulk_leaderboard;
DROP TABLE vip_users;
