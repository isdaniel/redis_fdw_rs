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
--   Part 5: Parameterized JOIN with join_batch_size + per-param
--             cache. Today's NestLoop drives one outer row at a
--             time, so the fast path issues a single direct
--             HGET / SISMEMBER / ZSCORE / GET per cache miss
--             (no pipeline needed). join_batch_size caps the
--             per-param cache to avoid unbounded memory growth;
--             the pipelined HMGET / SISMEMBER / ZSCORE / MGET
--             path stays ready for when a future planner sends
--             multi-param batches. A/B with join_batch_size '1'
--             shows the cache effect.
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

--SELECT COUNT(*) FROM bulk_user_profiles_slow;

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

--SELECT COUNT(*) FROM bulk_user_profiles;

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

--SELECT COUNT(*) FROM bulk_active_users;

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

EXPLAIN (ANALYZE, VERBOSE)
SELECT v.user_id, p.value AS profile
FROM vip_users v
JOIN bulk_user_profiles p ON v.user_id = p.field
WHERE v.user_id = 'user:42';

-- Same shape — aggregation over the pushdown plan
EXPLAIN (ANALYZE, VERBOSE)
SELECT
    v.user_id,
    p.value AS profile
FROM vip_users v
JOIN bulk_user_profiles p ON v.user_id = p.field
ORDER BY v.user_id
LIMIT 10;

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


EXPLAIN (ANALYZE, VERBOSE)
SELECT a.member AS user_id, p.value AS profile
FROM bulk_active_users a
JOIN bulk_user_profiles p ON p.field = a.member
WHERE a.member IN (
    'user:1','user:42','user:100','user:777','user:9999',
    'user:12345','user:55555','user:88888','user:99999','user:99000'
);


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
-- EXPLAIN (ANALYZE, VERBOSE)
-- SELECT COUNT(*) FROM bulk_leaderboard;

-- Pushed score range — ZRANGEBYSCORE, fetches only top ~1%
EXPLAIN (ANALYZE, VERBOSE)
SELECT * FROM bulk_leaderboard
WHERE score >= 99000
ORDER BY score DESC;

-- ============================================================
-- PART 5: Batched parameterized JOIN — join_batch_size in action
-- ============================================================
-- New in PR-2: when the planner picks a parameterized nested-loop,
-- the FDW routes the inner lookup through batch_parameterized_lookup
-- with a per-param cache (capped at join_batch_size). NestLoop today
-- drives one outer row at a time, so the params.len()==1 fast path
-- issues a single direct command (HGET / SISMEMBER / ZSCORE / GET).
-- The pipelined fallback (HMGET / pipelined SISMEMBER / pipelined
-- ZSCORE / MGET) is kept ready for if/when the planner sends
-- multi-param batches.
--
-- EXPLAIN shows three new labels on the inner Foreign Scan:
--   Join Batch Size: 256 rows                  -- (cache cap)
--   Join Batch Mode: pipeline | fallback | n/a -- (multi-param mode)
--   Redis Ops: HGET, HMGET                     -- (single-param fast / multi-param fallback)
--   Pushdown In Join: <condition> (filtered after lookup)
--
-- Mode is "pipeline" on standalone, "fallback" on cluster
-- (ClusterConnection rejects multi-key pipelines), "n/a" otherwise.

-- 5A. Default join_batch_size (256) — fast-path HGET per cache miss
CREATE FOREIGN TABLE bulk_user_profiles_batched (field text, value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'hash',
        table_key_prefix 'demo11:users:profiles',  -- reuse PART 1 data
        batch_size '10000',
        join_batch_size '256'
    );

EXPLAIN (ANALYZE, VERBOSE)
SELECT v.user_id, p.value
FROM vip_users v
JOIN bulk_user_profiles_batched p ON p.field = v.user_id;

-- 5C. ZSet score-range under a join — uses ZSCORE per outer row
--     (single direct command in the params.len()==1 fast-path).
--     The score range is applied as a post-fetch filter.
--     EXPLAIN shows "Redis Ops: ZSCORE" and
--     "Pushdown In Join: score >= '50000' (filtered after lookup)".
--     ZRANGEBYSCORE is intentionally NOT used in the per-param join
--     path — it would fetch the whole range per outer row, which is
--     catastrophically slow on low-selectivity ranges.
CREATE FOREIGN TABLE bulk_leaderboard_batched (member text, score float8)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'zset',
        table_key_prefix 'demo11:leaderboard',
        batch_size '10000',
        join_batch_size '256'
    );

CREATE TEMPORARY TABLE leaderboard_picks (player_id text PRIMARY KEY);
INSERT INTO leaderboard_picks
SELECT 'player:' || g::text FROM generate_series(1, 200) g;

EXPLAIN (ANALYZE, VERBOSE)
SELECT lp.player_id, lb.score
FROM leaderboard_picks lp
JOIN bulk_leaderboard_batched lb ON lb.member = lp.player_id
WHERE lb.score >= 50000;

EXPLAIN (ANALYZE, VERBOSE)
SELECT *
FROM  bulk_leaderboard_batched lb 
WHERE lb.score between 50000 and 60000;

-- ============================================================
-- Cleanup
-- ============================================================
TRUNCATE bulk_user_profiles_slow;
TRUNCATE bulk_user_profiles;
TRUNCATE bulk_active_users;
TRUNCATE bulk_leaderboard;
DROP FOREIGN TABLE bulk_user_profiles_slow;
DROP FOREIGN TABLE bulk_user_profiles;
DROP FOREIGN TABLE bulk_user_profiles_batched;
DROP FOREIGN TABLE IF EXISTS bulk_user_profiles_unbatched;
DROP FOREIGN TABLE bulk_active_users;
DROP FOREIGN TABLE bulk_leaderboard;
DROP FOREIGN TABLE bulk_leaderboard_batched;
DROP TABLE vip_users;
DROP TABLE leaderboard_picks;
