-- pgbench script for zset table operations

\set member random(1, 100)
\set score random(1, 255)

BEGIN;
INSERT INTO redis_zset (member, score) VALUES ('member-' || :member, :score);
SELECT * FROM redis_zset WHERE member = 'member-' || :member;
--UPDATE redis_zset SET score = :score + 100 WHERE member = 'member-' || :member;
DELETE FROM redis_zset WHERE member = 'member-' || :member;
COMMIT;
