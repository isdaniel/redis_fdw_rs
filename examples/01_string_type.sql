-- ============================================================
-- Demo 1: String Type - Simple Key-Value Storage
-- ============================================================
-- Redis String is the most basic type: one key → one value
-- Mapped to a single-column foreign table
-- ============================================================

-- Create a foreign table backed by a Redis STRING key
CREATE FOREIGN TABLE demo_config (value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'string',
        table_key_prefix 'app:config:version'
    );

-- INSERT sets the Redis key
INSERT INTO demo_config VALUES ('2.1.0');

-- SELECT reads the key
SELECT * FROM demo_config;

-- UPDATE modifies the value
UPDATE demo_config SET value = '2.2.0';
SELECT * FROM demo_config;

-- DELETE removes the key from Redis
DELETE FROM demo_config;
SELECT * FROM demo_config;  -- returns empty

-- Cleanup
DROP FOREIGN TABLE demo_config;
