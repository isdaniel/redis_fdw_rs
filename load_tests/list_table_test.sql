-- pgbench script for list table operations
-- Tests INSERT, SELECT, DELETE on Redis list foreign table

\set element random(1, 100)

INSERT INTO redis_list (element) VALUES ('element-' || :element);
SELECT * FROM redis_list WHERE element = 'element-' || :element;
DELETE FROM redis_list WHERE element = 'element-' || :element;
