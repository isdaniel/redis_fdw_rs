-- pgbench script for zset table operations
-- Tests INSERT, SELECT, DELETE on Redis sorted set foreign table

\set member random(1, 100)
\set score random(1, 255)

INSERT INTO redis_zset (member, score) VALUES ('member-' || :member, :score);
SELECT * FROM redis_zset WHERE member = 'member-' || :member;
DELETE FROM redis_zset WHERE member = 'member-' || :member;
