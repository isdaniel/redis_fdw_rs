-- pgbench script for set table operations

\set id random(1, 100000 * :scale)
\set member random(1, 100)

BEGIN;
INSERT INTO redis_set (key, member) VALUES ('set:' || :id, 'member-' || :member);
SELECT * FROM redis_set WHERE key = 'set:' || :id;
DELETE FROM redis_set WHERE key = 'set:' || :id AND member = 'member-' || :member;
COMMIT;
