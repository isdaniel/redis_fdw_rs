-- pgbench script for write-heavy operations
-- 80% writes, 20% reads across Redis foreign tables

\set id random(1, 100000 * :scale)
\set hash_field random(1, 100)
\set list_element random(1, 100)
\set set_member random(1, 100)

INSERT INTO redis_string (value) VALUES ('value-' || :id);
INSERT INTO redis_hash (field, value) VALUES ('field-' || :hash_field, 'value-' || :id);
SELECT * FROM redis_string WHERE value = 'value-' || :id;
DELETE FROM redis_string WHERE value = 'value-' || :id;
