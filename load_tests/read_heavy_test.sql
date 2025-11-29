-- pgbench script for read-heavy operations
-- 80% reads, 20% writes across Redis foreign tables

\set id random(1, 100000 * :scale)
\set hash_field random(1, 100)

SELECT * FROM redis_string;
SELECT * FROM redis_hash WHERE field = 'field-' || :hash_field;
SELECT * FROM redis_list;
SELECT * FROM redis_set;
INSERT INTO redis_string VALUES ('value-' || :id);
