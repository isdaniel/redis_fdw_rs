-- ============================================================
-- Demo 9: WHERE Clause Pushdown Optimization
-- ============================================================
-- The FDW pushes WHERE conditions to Redis when possible:
--   Hash:  field = 'x'  → HGET (instead of HGETALL)
--   Set:   member = 'x' → SISMEMBER (instead of SMEMBERS)
--   ZSet:  score >= N   → ZRANGEBYSCORE
--   List:  index = N    → LINDEX
-- This dramatically reduces data transfer for selective queries.
-- ============================================================

\timing on

-- Setup: Hash table with many fields
CREATE FOREIGN TABLE demo_product (field text, value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'hash',
        table_key_prefix 'product:sku:12345'
    );

INSERT INTO demo_product VALUES ('name', 'Mechanical Keyboard');
INSERT INTO demo_product VALUES ('price', '129.99');
INSERT INTO demo_product VALUES ('brand', 'KeyCraft');
INSERT INTO demo_product VALUES ('stock', '42');
INSERT INTO demo_product VALUES ('category', 'electronics');
INSERT INTO demo_product VALUES ('weight_kg', '0.85');

-- Without pushdown: fetches ALL fields, filters in PG
EXPLAIN (VERBOSE) SELECT * FROM demo_product;

-- With pushdown: fetches ONLY the requested field via HGET
EXPLAIN (VERBOSE) SELECT value FROM demo_product WHERE field = 'price';
SELECT value FROM demo_product WHERE field = 'price';

-- IN operator pushdown: uses HMGET for multiple fields
EXPLAIN (VERBOSE) SELECT * FROM demo_product WHERE field IN ('name', 'price', 'stock');
SELECT * FROM demo_product WHERE field IN ('name', 'price', 'stock');

-- ============================================================
-- ZSet score range pushdown
-- ============================================================
CREATE FOREIGN TABLE demo_scores (member text, score numeric)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'zset',
        table_key_prefix 'demo:scores:range'
    );

INSERT INTO demo_scores VALUES ('a', 10);
INSERT INTO demo_scores VALUES ('b', 20);
INSERT INTO demo_scores VALUES ('c', 30);
INSERT INTO demo_scores VALUES ('d', 40);
INSERT INTO demo_scores VALUES ('e', 50);

-- Range query pushed to ZRANGEBYSCORE
EXPLAIN (VERBOSE) SELECT * FROM demo_scores WHERE score >= 25 AND score <= 45;
SELECT * FROM demo_scores WHERE score >= 25 AND score <= 45;

-- Cleanup
DROP FOREIGN TABLE demo_product;
DROP FOREIGN TABLE demo_scores;
