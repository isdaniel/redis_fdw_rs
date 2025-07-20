-- pgbench script for read-heavy operations

\set id random(1, 100000 * :scale)

-- 80% reads, 20% writes
BEGIN;
SELECT * FROM redis_string;
SELECT * FROM redis_hash;
SELECT * FROM redis_list;
SELECT * FROM redis_set;
INSERT INTO redis_string VALUES ('value-' || :id);
COMMIT;
