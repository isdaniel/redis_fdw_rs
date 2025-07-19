-- pgbench script for list table operations

\set id random(1, 100000 * :scale)
\set element random(1, 100)

BEGIN;
INSERT INTO redis_list (key, element) VALUES ('list:' || :id, 'element-' || :element);
SELECT * FROM redis_list WHERE key = 'list:' || :id;
UPDATE redis_list SET element = 'updated-element-' || :element WHERE key = 'list:' || :id AND element = 'element-' || :element;
DELETE FROM redis_list WHERE key = 'list:' || :id AND element = 'updated-element-' || :element;
COMMIT;
