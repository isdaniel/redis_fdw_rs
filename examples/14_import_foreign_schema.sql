-- ============================================================
-- Demo 14: IMPORT FOREIGN SCHEMA
-- ============================================================
-- IMPORT FOREIGN SCHEMA auto-discovers Redis keys matching a glob
-- pattern, detects each key's type, and generates the corresponding
-- CREATE FOREIGN TABLE DDL automatically.
-- ============================================================

\timing on

-- ------------------------------------------------------------
-- Step 1: Seed some Redis keys of different types
-- ------------------------------------------------------------
CREATE FOREIGN TABLE demo_import_s1 (value text)
    SERVER redis_server
    OPTIONS (database '0', table_type 'string', table_key_prefix 'import:app:config');

CREATE FOREIGN TABLE demo_import_s2 (field text, value text)
    SERVER redis_server
    OPTIONS (database '0', table_type 'hash', table_key_prefix 'import:app:settings');

CREATE FOREIGN TABLE demo_import_s3 (member text)
    SERVER redis_server
    OPTIONS (database '0', table_type 'set', table_key_prefix 'import:app:flags');

INSERT INTO demo_import_s1 VALUES ('v1.0');
INSERT INTO demo_import_s2 VALUES ('theme', 'dark');
INSERT INTO demo_import_s3 VALUES ('beta');

-- Drop the seeding tables; the keys remain in Redis
DROP FOREIGN TABLE demo_import_s1;
DROP FOREIGN TABLE demo_import_s2;
DROP FOREIGN TABLE demo_import_s3;

-- ------------------------------------------------------------
-- Step 2: IMPORT - the FDW discovers the keys and types
-- ------------------------------------------------------------
IMPORT FOREIGN SCHEMA "import:*"
    FROM SERVER redis_server
    INTO public;

-- List the tables the FDW created for us
SELECT relname FROM pg_class
    WHERE relkind = 'f' AND relname LIKE 'import%'
    ORDER BY relname;

-- Query them as ordinary foreign tables
SELECT * FROM "import:app:config";
SELECT * FROM "import:app:settings";
SELECT * FROM "import:app:flags";

-- ------------------------------------------------------------
-- Cleanup
-- ------------------------------------------------------------
DROP FOREIGN TABLE IF EXISTS "import:app:config";
DROP FOREIGN TABLE IF EXISTS "import:app:settings";
DROP FOREIGN TABLE IF EXISTS "import:app:flags";
