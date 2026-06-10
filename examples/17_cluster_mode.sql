-- ============================================================
-- Demo 17: Cluster Mode
-- ============================================================
-- Redis FDW supports Redis Cluster (sharded multi-node).
-- Specify multiple nodes in host_port (comma-separated) and the
-- driver auto-discovers the full topology via CLUSTER SLOTS.
-- All operations work the same way — slot routing is transparent.
--
-- Prerequisites: make setup-redis (starts cluster on ports 7001-7006)
-- Cluster topology: 3 masters (7001-7003) + 3 replicas (7004-7006)
--
-- Cluster Limitations:
--   - Multi-key SCAN (SELECT * from pattern tables) is not supported
--     because SCAN cannot be routed across cluster slots.
--   - Use key pushdown (WHERE key = 'x' or key IN (...)) instead.
--   - Multi-key TRUNCATE also relies on SCAN and is not supported.
-- ============================================================

-- ============================================================
-- Part 1: Cluster connection with multiple seed nodes
-- ============================================================
-- Comma-separated host_port auto-enables cluster mode.
-- Listing multiple nodes provides failover if one seed is down.

CREATE SERVER redis_cluster
    FOREIGN DATA WRAPPER redis_wrapper
    OPTIONS (
        host_port '127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006'
    );

-- ============================================================
-- Part 2: All table types on cluster (multi-node host_port)
-- ============================================================

-- 2a. Hash table
CREATE FOREIGN TABLE cluster_hash (field text, value text)
    SERVER redis_cluster
    OPTIONS (
        table_type 'hash',
        table_key_prefix 'cluster:app:config'
    );

INSERT INTO cluster_hash VALUES ('version', '3.2.1');
INSERT INTO cluster_hash VALUES ('env', 'production');
INSERT INTO cluster_hash VALUES ('region', 'us-west-2');
INSERT INTO cluster_hash VALUES ('max_workers', '16');

SELECT * FROM cluster_hash;
SELECT value FROM cluster_hash WHERE field = 'env';
SELECT value FROM cluster_hash WHERE field IN ('version', 'region');

UPDATE cluster_hash SET value = '3.3.0' WHERE field = 'version';
SELECT value FROM cluster_hash WHERE field = 'version';

DELETE FROM cluster_hash WHERE field = 'max_workers';
SELECT * FROM cluster_hash;

-- 2b. Set table
CREATE FOREIGN TABLE cluster_set (member text)
    SERVER redis_cluster
    OPTIONS (
        table_type 'set',
        table_key_prefix 'cluster:services:active'
    );

INSERT INTO cluster_set VALUES ('api-gateway');
INSERT INTO cluster_set VALUES ('auth-service');
INSERT INTO cluster_set VALUES ('payment-service');
INSERT INTO cluster_set VALUES ('notification-service');
INSERT INTO cluster_set VALUES ('analytics-worker');

SELECT * FROM cluster_set;
SELECT * FROM cluster_set WHERE member = 'auth-service';

DELETE FROM cluster_set WHERE member = 'analytics-worker';
SELECT * FROM cluster_set;

-- 2c. Sorted set
CREATE FOREIGN TABLE cluster_zset (member text, score numeric)
    SERVER redis_cluster
    OPTIONS (
        table_type 'zset',
        table_key_prefix 'cluster:leaderboard:daily'
    );

INSERT INTO cluster_zset VALUES ('player:alice', 2850);
INSERT INTO cluster_zset VALUES ('player:bob', 3200);
INSERT INTO cluster_zset VALUES ('player:carol', 1900);
INSERT INTO cluster_zset VALUES ('player:dave', 4100);
INSERT INTO cluster_zset VALUES ('player:eve', 3750);

SELECT * FROM cluster_zset;

-- Score range pushdown → ZRANGEBYSCORE
SELECT * FROM cluster_zset WHERE score >= 3000 AND score <= 4000;

-- Member pushdown → ZSCORE
SELECT * FROM cluster_zset WHERE member = 'player:dave';

UPDATE cluster_zset SET score = 4200 WHERE member = 'player:dave';
SELECT * FROM cluster_zset WHERE member = 'player:dave';

-- 2d. List table
CREATE FOREIGN TABLE cluster_list (element text)
    SERVER redis_cluster
    OPTIONS (
        table_type 'list',
        table_key_prefix 'cluster:queue:tasks'
    );

INSERT INTO cluster_list VALUES ('task:compress-logs');
INSERT INTO cluster_list VALUES ('task:send-reports');
INSERT INTO cluster_list VALUES ('task:cleanup-tmp');
INSERT INTO cluster_list VALUES ('task:rotate-keys');

SELECT * FROM cluster_list;

-- 2e. String table
CREATE FOREIGN TABLE cluster_string (value text)
    SERVER redis_cluster
    OPTIONS (
        table_type 'string',
        table_key_prefix 'cluster:feature:dark-mode'
    );

INSERT INTO cluster_string VALUES ('{"enabled": true, "rollout_pct": 75}');
SELECT * FROM cluster_string;

UPDATE cluster_string SET value = '{"enabled": true, "rollout_pct": 100}';
SELECT * FROM cluster_string;

-- 2f. Stream table
CREATE FOREIGN TABLE cluster_stream (stream_id text, event_type text, payload text)
    SERVER redis_cluster
    OPTIONS (
        table_type 'stream',
        table_key_prefix 'cluster:events:audit'
    );

INSERT INTO cluster_stream VALUES ('*', 'user.login', '{"user":"alice","ip":"10.0.1.5"}');
INSERT INTO cluster_stream VALUES ('*', 'user.login', '{"user":"bob","ip":"10.0.1.8"}');
INSERT INTO cluster_stream VALUES ('*', 'config.update', '{"key":"max_workers","old":"8","new":"16"}');
INSERT INTO cluster_stream VALUES ('*', 'user.logout', '{"user":"alice"}');

SELECT * FROM cluster_stream;

-- ============================================================
-- Part 3: Multi-key pattern mode on cluster (pushdown only)
-- ============================================================
-- In cluster mode, full-table SCAN is not supported.
-- However, key pushdown (=, IN) works perfectly because
-- it routes directly to the correct shard by hash slot.

-- 3a. String multi-key: session store spread across shards
CREATE FOREIGN TABLE cluster_sessions (key text, value text)
    SERVER redis_cluster
    OPTIONS (
        table_type 'string',
        table_key_prefix 'csession:*'
    );

INSERT INTO cluster_sessions VALUES ('csession:u1001', '{"user":"alice","role":"admin"}');
INSERT INTO cluster_sessions VALUES ('csession:u1002', '{"user":"bob","role":"viewer"}');
INSERT INTO cluster_sessions VALUES ('csession:u1003', '{"user":"carol","role":"editor"}');
INSERT INTO cluster_sessions VALUES ('csession:u1004', '{"user":"dave","role":"viewer"}');
INSERT INTO cluster_sessions VALUES ('csession:u1005', '{"user":"eve","role":"admin"}');

-- Key pushdown: exact match (direct GET, no SCAN)
SELECT * FROM cluster_sessions WHERE key = 'csession:u1003';

-- Key pushdown: IN list (pipeline GET across shards)
SELECT * FROM cluster_sessions WHERE key IN ('csession:u1001', 'csession:u1005');

-- NOTE: SELECT * (full SCAN) and LIKE are NOT supported in cluster mode, because they rely on SCAN which cannot be routed across slots.
-- SELECT * FROM cluster_sessions;                          -- ERROR: SCAN cannot route
-- SELECT * FROM cluster_sessions WHERE key LIKE 'c%';     -- ERROR: narrowed SCAN

DELETE FROM cluster_sessions WHERE key = 'csession:u1004';
SELECT * FROM cluster_sessions WHERE key = 'csession:u1004';  -- should be empty

-- 3b. Hash multi-key: per-user profiles distributed across shards
CREATE FOREIGN TABLE cluster_profiles (key text, field text, value text)
    SERVER redis_cluster
    OPTIONS (
        table_type 'hash',
        table_key_prefix 'cprofile:*'
    );

INSERT INTO cluster_profiles VALUES ('cprofile:alice', 'email', 'alice@example.com');
INSERT INTO cluster_profiles VALUES ('cprofile:alice', 'team', 'platform');
INSERT INTO cluster_profiles VALUES ('cprofile:bob', 'email', 'bob@example.com');
INSERT INTO cluster_profiles VALUES ('cprofile:bob', 'team', 'frontend');
INSERT INTO cluster_profiles VALUES ('cprofile:carol', 'email', 'carol@example.com');
INSERT INTO cluster_profiles VALUES ('cprofile:carol', 'team', 'data');

-- Key pushdown: exact key lookup
SELECT * FROM cluster_profiles WHERE key = 'cprofile:alice';
SELECT * FROM cluster_profiles WHERE key = 'cprofile:bob';

-- Multi-key IN pushdown
SELECT * FROM cluster_profiles WHERE key IN ('cprofile:alice', 'cprofile:carol');

-- 3c. Set multi-key: tags per resource
CREATE FOREIGN TABLE cluster_tags (key text, member text)
    SERVER redis_cluster
    OPTIONS (
        table_type 'set',
        table_key_prefix 'ctag:*'
    );

INSERT INTO cluster_tags VALUES ('ctag:post:1', 'rust');
INSERT INTO cluster_tags VALUES ('ctag:post:1', 'postgresql');
INSERT INTO cluster_tags VALUES ('ctag:post:1', 'fdw');
INSERT INTO cluster_tags VALUES ('ctag:post:2', 'docker');
INSERT INTO cluster_tags VALUES ('ctag:post:2', 'redis');
INSERT INTO cluster_tags VALUES ('ctag:post:3', 'rust');
INSERT INTO cluster_tags VALUES ('ctag:post:3', 'performance');

-- Key pushdown
SELECT * FROM cluster_tags WHERE key = 'ctag:post:1';
SELECT * FROM cluster_tags WHERE key IN ('ctag:post:2', 'ctag:post:3');

-- 3d. ZSet multi-key: per-game leaderboards
CREATE FOREIGN TABLE cluster_game_scores (key text, member text, score numeric)
    SERVER redis_cluster
    OPTIONS (
        table_type 'zset',
        table_key_prefix 'cgame:scores:*'
    );

INSERT INTO cluster_game_scores VALUES ('cgame:scores:round1', 'alice', 150);
INSERT INTO cluster_game_scores VALUES ('cgame:scores:round1', 'bob', 230);
INSERT INTO cluster_game_scores VALUES ('cgame:scores:round2', 'alice', 310);
INSERT INTO cluster_game_scores VALUES ('cgame:scores:round2', 'carol', 280);

-- Key pushdown
SELECT * FROM cluster_game_scores WHERE key = 'cgame:scores:round1';
SELECT * FROM cluster_game_scores WHERE key IN ('cgame:scores:round1', 'cgame:scores:round2');

-- ============================================================
-- Part 4: TTL support on cluster
-- ============================================================

-- 4a. Table-level default TTL (single-key, works fine)
CREATE FOREIGN TABLE cluster_cache (value text)
    SERVER redis_cluster
    OPTIONS (
        table_type 'string',
        table_key_prefix 'cluster:cache:homepage',
        ttl '600'
    );

INSERT INTO cluster_cache VALUES ('<html>cached cluster page</html>');
SELECT * FROM cluster_cache;

-- 4b. Per-row TTL override via ttl column (multi-key, use pushdown)
CREATE FOREIGN TABLE cluster_cache_ttl (key text, value text, ttl bigint)
    SERVER redis_cluster
    OPTIONS (
        table_type 'string',
        table_key_prefix 'cluster:ttlcache:*',
        ttl '300'
    );

INSERT INTO cluster_cache_ttl VALUES ('cluster:ttlcache:hot', '{"data":"expires-fast"}', 30);
INSERT INTO cluster_cache_ttl VALUES ('cluster:ttlcache:warm', '{"data":"normal-expiry"}');
INSERT INTO cluster_cache_ttl VALUES ('cluster:ttlcache:cold', '{"data":"long-lived"}', 3600);
INSERT INTO cluster_cache_ttl VALUES ('cluster:ttlcache:permanent', '{"data":"no-expiry"}', -1);

-- Verify TTL via key pushdown
SELECT * FROM cluster_cache_ttl WHERE key = 'cluster:ttlcache:hot';
SELECT * FROM cluster_cache_ttl WHERE key = 'cluster:ttlcache:permanent';
SELECT * FROM cluster_cache_ttl WHERE key IN (
    'cluster:ttlcache:hot', 'cluster:ttlcache:warm',
    'cluster:ttlcache:cold', 'cluster:ttlcache:permanent'
);

-- ============================================================
-- Part 5: Batch insert on cluster (30000 keys)
-- ============================================================
-- Pipeline batching groups commands by hash slot and pipelines
-- within each node for maximum throughput.

-- Multi-key batch insert: 30000 keys distributed across shards
CREATE FOREIGN TABLE cluster_batch_strings (key text, value text)
    SERVER redis_cluster
    OPTIONS (
        table_type 'string',
        table_key_prefix 'cbatch:item:*',
        batch_size '10000'
    );

INSERT INTO cluster_batch_strings
SELECT
    'cbatch:item:' || g::text,
    '{"id":' || g::text || ',"shard":' || (g % 3)::text
        || ',"region":"' || CASE g % 4
            WHEN 0 THEN 'us-west-2'
            WHEN 1 THEN 'us-east-1'
            WHEN 2 THEN 'eu-west-1'
            ELSE 'ap-northeast-1'
        END || '"}'
FROM generate_series(1, 30000) g;

-- Point lookup (direct GET, no SCAN)
EXPLAIN ANALYZE SELECT * FROM cluster_batch_strings WHERE key = 'cbatch:item:15000';

-- Multi-key lookup (pipeline GET across shards)
EXPLAIN ANALYZE SELECT * FROM cluster_batch_strings WHERE key IN (
    'cbatch:item:1', 'cbatch:item:10000', 'cbatch:item:20000', 'cbatch:item:30000'
);

-- Verify distribution: sample from different key ranges
EXPLAIN ANALYZE SELECT * FROM cluster_batch_strings WHERE key IN (
    'cbatch:item:7777', 'cbatch:item:14444', 'cbatch:item:21111', 'cbatch:item:28888'
) 
--ORDER BY key
LIMIT 2;


-- ============================================================
-- Part 7: TRUNCATE on cluster (single-key only)
-- ============================================================
-- Single-key TRUNCATE uses DEL/UNLINK which routes by slot — works fine.
-- Multi-key TRUNCATE relies on SCAN and is NOT supported in cluster mode.

CREATE FOREIGN TABLE cluster_trunc_hash (field text, value text)
    SERVER redis_cluster
    OPTIONS (
        table_type 'hash',
        table_key_prefix 'cluster:trunc:data'
    );

INSERT INTO cluster_trunc_hash VALUES ('a', '1');
INSERT INTO cluster_trunc_hash VALUES ('b', '2');
INSERT INTO cluster_trunc_hash VALUES ('c', '3');
SELECT * FROM cluster_trunc_hash;

TRUNCATE cluster_trunc_hash;
SELECT * FROM cluster_trunc_hash;


-- ============================================================
-- Cleanup
-- ============================================================
DROP FOREIGN TABLE IF EXISTS cluster_hash;
DROP FOREIGN TABLE IF EXISTS cluster_set;
DROP FOREIGN TABLE IF EXISTS cluster_zset;
DROP FOREIGN TABLE IF EXISTS cluster_list;
DROP FOREIGN TABLE IF EXISTS cluster_string;
DROP FOREIGN TABLE IF EXISTS cluster_stream;
DROP FOREIGN TABLE IF EXISTS cluster_sessions;
DROP FOREIGN TABLE IF EXISTS cluster_profiles;
DROP FOREIGN TABLE IF EXISTS cluster_tags;
DROP FOREIGN TABLE IF EXISTS cluster_game_scores;
DROP FOREIGN TABLE IF EXISTS cluster_cache;
DROP FOREIGN TABLE IF EXISTS cluster_cache_ttl;
DROP FOREIGN TABLE IF EXISTS cluster_batch_strings;
DROP FOREIGN TABLE IF EXISTS cluster_verify_write;
DROP FOREIGN TABLE IF EXISTS cluster_verify_read;
DROP FOREIGN TABLE IF EXISTS cluster_trunc_hash;
DROP SERVER IF EXISTS redis_cluster;
