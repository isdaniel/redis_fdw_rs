-- pgbench script for write-heavy operations

\set hash_field random(1, 100)
\set list_element random(1, 100)
\set set_member random(1, 100)

-- 80% writes, 20% reads
BEGIN;
INSERT INTO redis_string (value) VALUES ('value-' || :id);
INSERT INTO redis_hash (field, value) VALUES ('field-' || :hash_field, 'value-' || :id);
--UPDATE redis_string SET value = 'updated-value-' || :id WHERE value = 'value-' || :id;
DELETE FROM redis_string WHERE value = 'updated-value-' || :id;
SELECT * FROM redis_string WHERE value = 'updated-value-' || :id;
COMMIT;
