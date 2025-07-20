-- pgbench script for list table operations

\set element random(1, 100)

BEGIN;
INSERT INTO redis_list (element) VALUES ('element-' || :element);
SELECT * FROM redis_list WHERE element = 'element-' || :element;
--UPDATE redis_list SET element = 'updated-element-' || :element WHERE element = 'element-' || :element;
DELETE FROM redis_list WHERE element = 'updated-element-' || :element;
COMMIT;
