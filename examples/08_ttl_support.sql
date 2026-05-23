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

-- Cleanup
DROP FOREIGN TABLE demo_cache_persist;
