-- ============================================================
-- Demo 7: Multi-Key Pattern Mode
-- ============================================================
-- When table_key_prefix contains a glob pattern (*, ?, []),
-- the FDW scans multiple Redis keys as rows.
-- First column = Redis key name, remaining columns = value data
-- ============================================================

-- First, seed some Redis keys using a helper table
CREATE FOREIGN TABLE demo_session_seed (value text)
    SERVER redis_server
    OPTIONS (database '0', table_type 'string', table_key_prefix 'session:user101');
INSERT INTO demo_session_seed VALUES ('{"logged_in": true, "ip": "10.0.0.1"}');
DROP FOREIGN TABLE demo_session_seed;

CREATE FOREIGN TABLE demo_session_seed2 (value text)
    SERVER redis_server
    OPTIONS (database '0', table_type 'string', table_key_prefix 'session:user102');
INSERT INTO demo_session_seed2 VALUES ('{"logged_in": true, "ip": "10.0.0.2"}');
DROP FOREIGN TABLE demo_session_seed2;

CREATE FOREIGN TABLE demo_session_seed3 (value text)
    SERVER redis_server
    OPTIONS (database '0', table_type 'string', table_key_prefix 'session:user103');
INSERT INTO demo_session_seed3 VALUES ('{"logged_in": false, "ip": "10.0.0.3"}');
DROP FOREIGN TABLE demo_session_seed3;

-- Now create a multi-key pattern table (note the * in the prefix)
CREATE FOREIGN TABLE demo_sessions (key text, value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'string',
        table_key_prefix 'session:*'
    );

-- SELECT scans all keys matching the pattern
EXPLAIN (ANALYZE, VERBOSE)
SELECT * FROM demo_sessions;

-- Use PostgreSQL filtering on the results
EXPLAIN (ANALYZE, VERBOSE)
SELECT key, value FROM demo_sessions WHERE key LIKE '%user101%';

-- INSERT into a specific key (first column = key name)
EXPLAIN (ANALYZE, VERBOSE)
INSERT INTO demo_sessions VALUES ('session:user104', '{"logged_in": true, "ip": "10.0.0.4"}');

-- DELETE a specific key
DELETE FROM demo_sessions WHERE key = 'session:user104';
SELECT * FROM demo_sessions;

-- Cleanup
DROP FOREIGN TABLE demo_sessions;
