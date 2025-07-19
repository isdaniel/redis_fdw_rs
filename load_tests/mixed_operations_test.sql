-- pgbench script for mixed read-write operations on Redis FDW

\set id random(1, 100000 * :scale)
\set hash_field random(1, 100)
\set list_element random(1, 100)
\set set_member random(1, 100)
\set zset_member random(1, 100)
\set zset_score random(1, 1000)

-- String operations
BEGIN;
INSERT INTO redis_string (value) VALUES ('value-' || :id);
SELECT * FROM redis_string WHERE key = 'pgbench:string:' || :id;
UPDATE redis_string SET value = 'updated-value-' || :id WHERE key = 'pgbench:string:' || :id;
DELETE FROM redis_string WHERE key = 'pgbench:string:' || :id;
COMMIT;

-- Hash operations
BEGIN;
INSERT INTO redis_hash (key, field, value) VALUES ('hash:' || :id, 'field-' || :hash_field, 'value-' || :id);
SELECT * FROM redis_hash WHERE key = 'hash:' || :id;
UPDATE redis_hash SET value = 'updated-value-' || :id WHERE key = 'hash:' || :id AND field = 'field-' || :hash_field;
DELETE FROM redis_hash WHERE key = 'hash:' || :id AND field = 'field-' || :hash_field;
COMMIT;

-- List operations
BEGIN;
INSERT INTO redis_list (key, element) VALUES ('list:' || :id, 'element-' || :list_element);
SELECT * FROM redis_list WHERE key = 'list:' || :id;
-- Note: Update/Delete for lists operate on values, not indices, in this FDW
-- This might not be typical for a list but follows the FDW implementation.
UPDATE redis_list SET element = 'updated-element-' || :list_element WHERE key = 'list:' || :id AND element = 'element-' || :list_element;
DELETE FROM redis_list WHERE key = 'list:' || :id AND element = 'updated-element-' || :list_element;
COMMIT;

-- Set operations
BEGIN;
INSERT INTO redis_set (key, member) VALUES ('set:' || :id, 'member-' || :set_member);
SELECT * FROM redis_set WHERE key = 'set:' || :id;
-- Note: Sets don't have updates, you remove and add.
-- Simulating update by delete and insert.
DELETE FROM redis_set WHERE key = 'set:' || :id AND member = 'member-' || :set_member;
INSERT INTO redis_set (key, member) VALUES ('set:' || :id, 'updated-member-' || :set_member);
DELETE FROM redis_set WHERE key = 'set:' || :id AND member = 'updated-member-' || :set_member;
COMMIT;

-- ZSet operations
BEGIN;
INSERT INTO redis_zset (key, member, score) VALUES ('zset:' || :id, 'member-' || :zset_member, :zset_score);
SELECT * FROM redis_zset WHERE key = 'zset:' || :id;
UPDATE redis_zset SET score = :zset_score + 100 WHERE key = 'zset:' || :id AND member = 'member-' || :zset_member;
DELETE FROM redis_zset WHERE key = 'zset:' || :id AND member = 'member-' || :zset_member;
COMMIT;
