-- pgbench script for zset table operations

\set id random(1, 100000 * :scale)
\set member random(1, 100)
\set score random(1, 1000)

BEGIN;
INSERT INTO redis_zset (key, member, score) VALUES ('zset:' || :id, 'member-' || :member, :score);
SELECT * FROM redis_zset WHERE key = 'zset:' || :id;
UPDATE redis_zset SET score = :score + 100 WHERE key = 'zset:' || :id AND member = 'member-' || :member;
DELETE FROM redis_zset WHERE key = 'zset:' || :id AND member = 'member-' || :member;
COMMIT;
