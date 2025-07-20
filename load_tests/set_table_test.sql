-- pgbench script for set table operations

\set member random(1, 100)

BEGIN;
INSERT INTO redis_set (member) VALUES ('member-' || :member);
SELECT * FROM redis_set WHERE member = 'member-' || :member;
DELETE FROM redis_set WHERE member = 'member-' || :member;
COMMIT;
