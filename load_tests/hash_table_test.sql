-- pgbench script for hash table operations

\set id random(1, 100000 * :scale)
\set field random(1, 100)

BEGIN;
INSERT INTO redis_hash ( field, value) VALUES ('field-' || :field, 'value-' || :id);
SELECT * FROM redis_hash WHERE field = 'field-' || :field;
DELETE FROM redis_hash WHERE field = 'field-' || :field;
COMMIT;
