-- pgbench script for string table operations
-- Tests INSERT, SELECT, DELETE on Redis string foreign table

\set id random(1, 100000 * :scale)

INSERT INTO redis_string (value) VALUES ('value-' || :id);
SELECT * FROM redis_string WHERE value = 'value-' || :id;
DELETE FROM redis_string WHERE value = 'value-' || :id;
