-- ============================================================
-- Demo 2: Hash Type - Structured Records
-- ============================================================
-- Redis Hash maps to (field, value) pairs
-- Perfect for storing structured objects
-- ============================================================

-- Create a foreign table backed by a Redis HASH
CREATE FOREIGN TABLE demo_user_profile (field text, value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'hash',
        table_key_prefix 'user:1001'
    );

-- INSERT adds fields to the hash
INSERT INTO demo_user_profile VALUES ('name', 'Alice Chen');
INSERT INTO demo_user_profile VALUES ('email', 'alice@example.com');
INSERT INTO demo_user_profile VALUES ('role', 'engineer');
INSERT INTO demo_user_profile VALUES ('team', 'platform');

-- SELECT reads all hash fields
SELECT * FROM demo_user_profile;

-- WHERE clause pushdown: uses HGET instead of HGETALL + filter
-- (Check with EXPLAIN to see pushdown in action)
EXPLAIN (VERBOSE) SELECT value FROM demo_user_profile WHERE field = 'email';
SELECT value FROM demo_user_profile WHERE field = 'email';

-- UPDATE a specific field
UPDATE demo_user_profile SET value = 'senior engineer' WHERE field = 'role';
SELECT * FROM demo_user_profile;

-- DELETE a specific field
DELETE FROM demo_user_profile WHERE field = 'team';
SELECT * FROM demo_user_profile;

-- Cleanup
DROP FOREIGN TABLE demo_user_profile;
