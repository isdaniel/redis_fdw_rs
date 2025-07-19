-- pgbench script for write-heavy operations

\set id random(1, 100000 * :scale)
\set hash_field random(1, 100)
\set list_element random(1, 100)
\set set_member random(1, 100)

-- 80% writes, 20% reads
BEGIN;
INSERT INTO redis_string (value) VALUES ('value-' || :id);
INSERT INTO redis_hash (key, field, value) VALUES ('hash:' || :id, 'field-' || :hash_field, 'value-' || :id);
UPDATE redis_string SET value = 'updated-value-' || :id WHERE key = 'pgbench:string:' || :id;
DELETE FROM redis_string WHERE key = 'pgbench:string:' || :id;
SELECT * FROM redis_string WHERE key = 'pgbench:string:' || :id;
COMMIT;
