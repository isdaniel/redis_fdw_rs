-- pgbench script for set table operations
-- Tests INSERT, SELECT, DELETE on Redis set foreign table

\set member random(1, 100)

INSERT INTO redis_set (member) VALUES ('member-' || :member);
SELECT * FROM redis_set WHERE member = 'member-' || :member;
DELETE FROM redis_set WHERE member = 'member-' || :member;
