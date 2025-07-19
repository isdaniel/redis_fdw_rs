-- pgbench script for read-heavy operations

\set id random(1, 100000 * :scale)

-- 80% reads, 20% writes
BEGIN;
SELECT * FROM redis_string WHERE key = 'pgbench:string:' || :id;
SELECT * FROM redis_hash WHERE key = 'hash:' || :id;
SELECT * FROM redis_list WHERE key = 'list:' || :id;
SELECT * FROM redis_set WHERE key = 'set:' || :id;
INSERT INTO redis_string (value) VALUES ('value-' || :id);
COMMIT;
