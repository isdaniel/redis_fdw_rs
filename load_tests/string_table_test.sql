-- pgbench script for string table operations

\set id random(1, 100000 * :scale)

BEGIN;
INSERT INTO redis_string (value) VALUES ('value-' || :id);
SELECT * FROM redis_string WHERE key = 'pgbench:string:' || :id;
UPDATE redis_string SET value = 'updated-value-' || :id WHERE key = 'pgbench:string:' || :id;
DELETE FROM redis_string WHERE key = 'pgbench:string:' || :id;
COMMIT;
