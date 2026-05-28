-- ============================================================
-- Demo 14: IMPORT FOREIGN SCHEMA
-- ============================================================
-- IMPORT FOREIGN SCHEMA auto-discovers Redis keys matching a glob
-- pattern, groups them by shared prefix (up to last ':'), detects
-- each group's type, and generates a multi-key FOREIGN TABLE per group.
-- ============================================================

\timing on

-- ------------------------------------------------------------
-- Step 1: Seed some Redis keys of different types/prefixes
-- ------------------------------------------------------------
CREATE FOREIGN TABLE demo_import_s1 (value text)
    SERVER redis_server
    OPTIONS (database '0', table_type 'string', table_key_prefix 'import:config:version');

CREATE FOREIGN TABLE demo_import_s2 (value text)
    SERVER redis_server
    OPTIONS (database '0', table_type 'string', table_key_prefix 'import:config:env');

CREATE FOREIGN TABLE demo_import_s3 (field text, value text)
    SERVER redis_server
    OPTIONS (database '0', table_type 'hash', table_key_prefix 'import:user:1001');

CREATE FOREIGN TABLE demo_import_s4 (field text, value text)
    SERVER redis_server
    OPTIONS (database '0', table_type 'hash', table_key_prefix 'import:user:1002');

INSERT INTO demo_import_s1 VALUES ('v1.0');
INSERT INTO demo_import_s2 VALUES ('production');
INSERT INTO demo_import_s3 VALUES ('name', 'Alice');
INSERT INTO demo_import_s4 VALUES ('name', 'Bob');

-- Drop the seeding tables; the keys remain in Redis
DROP FOREIGN TABLE demo_import_s1;
DROP FOREIGN TABLE demo_import_s2;
DROP FOREIGN TABLE demo_import_s3;
DROP FOREIGN TABLE demo_import_s4;

-- ------------------------------------------------------------
-- Step 2: IMPORT - the FDW discovers the keys and types
-- Keys with same prefix group into one multi-key table:
--   import:config:version + import:config:env → import_config (string, multi-key)
--   import:user:1001 + import:user:1002 → import_user (hash, multi-key)
-- ------------------------------------------------------------
IMPORT FOREIGN SCHEMA "import:*"
    FROM SERVER redis_server
    INTO public;

-- List the tables the FDW created for us
SELECT relname FROM pg_class
    WHERE relkind = 'f' AND relname LIKE 'import%'
    ORDER BY relname;

-- Query the auto-created multi-key string table
SELECT * FROM import_config;

-- Query the auto-created hash table
SELECT * FROM import_user;

-- ------------------------------------------------------------
-- Cleanup
-- ------------------------------------------------------------
DROP FOREIGN TABLE IF EXISTS import_config;
DROP FOREIGN TABLE IF EXISTS import_user;
