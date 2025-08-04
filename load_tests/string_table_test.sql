-- pgbench script for string table operations


BEGIN;
INSERT INTO redis_string (value) VALUES ('value-' || :id);
SELECT * FROM redis_string WHERE value = 'pgbench:string:' || :id;
DELETE FROM redis_string WHERE value = 'pgbench:string:' || :id;
COMMIT;
