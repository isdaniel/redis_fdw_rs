-- ============================================================
-- Demo 4: Set Type - Unique Collections
-- ============================================================
-- Redis Set: unordered collection of unique strings
-- Great for tags, memberships, unique tracking
-- ============================================================

-- Create a foreign table backed by a Redis SET
CREATE FOREIGN TABLE demo_user_tags (member text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'set',
        table_key_prefix 'user:1001:tags'
    );

-- INSERT adds members (duplicates are ignored by Redis)
INSERT INTO demo_user_tags VALUES ('rust');
INSERT INTO demo_user_tags VALUES ('postgresql');
INSERT INTO demo_user_tags VALUES ('redis');
INSERT INTO demo_user_tags VALUES ('linux');
INSERT INTO demo_user_tags VALUES ('rust');  -- duplicate, will be ignored

-- SELECT all members
SELECT * FROM demo_user_tags;

-- WHERE clause pushdown: uses SISMEMBER for existence check
EXPLAIN (VERBOSE) SELECT * FROM demo_user_tags WHERE member = 'rust';
SELECT * FROM demo_user_tags WHERE member = 'rust';

-- DELETE a specific member
DELETE FROM demo_user_tags WHERE member = 'linux';
SELECT * FROM demo_user_tags;

-- Cleanup
DROP FOREIGN TABLE demo_user_tags;
