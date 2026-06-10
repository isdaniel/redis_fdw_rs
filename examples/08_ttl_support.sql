-- ============================================================
-- Demo 8: TTL (Time-To-Live) Support
-- ============================================================
-- Redis keys can have automatic expiration.
-- The FDW supports TTL via:
--   1. Table-level default (ttl option)
--   2. Per-row override (ttl column)
-- ============================================================

-- Table with a default TTL of 300 seconds (5 minutes)
CREATE FOREIGN TABLE demo_cache (value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'string',
        table_key_prefix 'cache:page:home',
        ttl '300'
    );

INSERT INTO demo_cache VALUES ('<html>cached homepage</html>');
SELECT * FROM demo_cache;

-- Check TTL from Redis side (use psql's \! or redis-cli):
-- redis-cli -p 8899 TTL cache:page:home
-- Should show ~300

DROP FOREIGN TABLE demo_cache;

-- ============================================================
-- Per-row TTL override using a ttl column
-- ============================================================

CREATE FOREIGN TABLE demo_cache_ttl (value text, ttl bigint)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'string',
        table_key_prefix 'cache:api:response'
    );

-- Insert with custom TTL (60 seconds)
INSERT INTO demo_cache_ttl VALUES ('{"data": "fresh"}', 60);
SELECT * FROM demo_cache_ttl;
-- The ttl column shows remaining seconds

-- Insert with no expiry (ttl = -1 means persist forever)
-- First change key for a new entry
DROP FOREIGN TABLE demo_cache_ttl;
CREATE FOREIGN TABLE demo_cache_persist (value text, ttl bigint)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'string',
        table_key_prefix 'cache:api:permanent'
    );
INSERT INTO demo_cache_persist VALUES ('{"data": "permanent"}', -1);
SELECT * FROM demo_cache_persist;

CREATE FOREIGN TABLE user_profile (field text, value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'hash',
        table_key_prefix 'user:1001'
    );

-- INSERT adds fields to the hash
INSERT INTO user_profile VALUES ('name', 'Alice Chen');
INSERT INTO user_profile VALUES ('email', 'alice@example.com');
INSERT INTO user_profile VALUES ('role', 'engineer');
INSERT INTO user_profile VALUES ('team', 'platform');

-- SELECT reads all hash fields
SELECT * FROM user_profile;


CREATE FOREIGN TABLE session_fdw (key text, value text, ttl bigint)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'string',
        table_key_prefix 'session:*',
        ttl '300'
    );

INSERT INTO session_fdw VALUES ('session:user1', '{"logged_in": true, "ip": "10.0.0.1"}',10);
INSERT INTO session_fdw VALUES ('session:user2', '{"logged_in": true, "ip": "10.0.0.2"}',20);
INSERT INTO session_fdw VALUES ('session:user3', '{"logged_in": true, "ip": "10.0.0.3"}');
-- redis-cli TTL session:user1 session:user2 session:user3 
SELECT * FROM session_fdw where key in ('session:user1','session:user2','session:user3');

-- Cleanup
DROP FOREIGN TABLE demo_cache_persist;
DROP FOREIGN TABLE session_fdw;