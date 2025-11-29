-- pgbench script for mixed read-write operations on Redis FDW
-- Tests INSERT, SELECT, DELETE across String and Hash tables

\set id random(1, 100000 * :scale)
\set hash_field random(1, 100)

-- String operations
INSERT INTO redis_string (value) VALUES ('value-' || :id);
SELECT * FROM redis_string WHERE value = 'value-' || :id;
DELETE FROM redis_string WHERE value = 'value-' || :id;

-- Hash operations  
INSERT INTO redis_hash (field, value) VALUES ('field-' || :hash_field, 'value-' || :id);
SELECT * FROM redis_hash WHERE field = 'field-' || :hash_field;
DELETE FROM redis_hash WHERE field = 'field-' || :hash_field;
