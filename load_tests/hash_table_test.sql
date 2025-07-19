-- pgbench script for hash table operations

\set id random(1, 100000 * :scale)
\set field random(1, 100)

BEGIN;
INSERT INTO redis_hash (key, field, value) VALUES ('hash:' || :id, 'field-' || :field, 'value-' || :id);
SELECT * FROM redis_hash WHERE key = 'hash:' || :id;
UPDATE redis_hash SET value = 'updated-value-' || :id WHERE key = 'hash:' || :id AND field = 'field-' || :field;
DELETE FROM redis_hash WHERE key = 'hash:' || :id AND field = 'field-' || :field;
COMMIT;
