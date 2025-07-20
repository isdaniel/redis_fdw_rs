-- pgbench script for mixed read-write operations on Redis FDW

\set id random(1, 100000 * :scale)
\set hash_field random(1, 100)
\set list_element random(1, 100)
\set set_member random(1, 100)
\set zset_member random(1, 100)
\set zset_score random(1, 255)

-- String operations
BEGIN;
INSERT INTO redis_string (value) VALUES ('value-' || :id);
SELECT * FROM redis_string WHERE value = 'value-' || :id;
--UPDATE redis_string SET value = 'updated-value-' || :id WHERE value = 'value-' || :id;
DELETE FROM redis_string WHERE value = 'value-' || :id;
COMMIT;

-- Hash operations
BEGIN;
INSERT INTO redis_hash (field, value) VALUES ('field-' || :hash_field, 'value-' || :id);
SELECT * FROM redis_hash WHERE field = 'field-' || :hash_field;
--UPDATE redis_hash SET value = 'updated-value-' || :id WHERE field = 'field-' || :hash_field;
DELETE FROM redis_hash WHERE field = 'field-' || :hash_field;
COMMIT;

-- -- List operations
-- BEGIN;
-- INSERT INTO redis_list (element) VALUES ('element-' || :list_element);
-- SELECT * FROM redis_list WHERE element = 'element-' || :list_element;
-- DELETE FROM redis_list WHERE element = 'updated-element-' || :list_element;
-- COMMIT;

-- -- Set operations
-- BEGIN;
-- INSERT INTO redis_set (member) VALUES ('member-' || :set_member);
-- SELECT * FROM redis_set WHERE member = 'member-' || :set_member;
-- -- Note: Sets don't have updates, you remove and add.
-- -- Simulating update by delete and insert.
-- DELETE FROM redis_set WHERE member = 'member-' || :set_member;
-- INSERT INTO redis_set (member) VALUES ('updated-member-' || :set_member);
-- DELETE FROM redis_set WHERE member = 'updated-member-' || :set_member;
-- COMMIT;

-- -- ZSet operations
-- BEGIN;
-- INSERT INTO redis_zset (member, score) VALUES ('member-' || :zset_member, :zset_score);
-- SELECT * FROM redis_zset WHERE member = 'member-' || :zset_member;
-- --UPDATE redis_zset SET score = :zset_score + 100 WHERE member = 'member-' || :zset_member;
-- DELETE FROM redis_zset WHERE member = 'member-' || :zset_member;
-- COMMIT;
