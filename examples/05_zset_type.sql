-- ============================================================
-- Demo 5: Sorted Set (ZSet) - Scored Rankings
-- ============================================================
-- Redis Sorted Set: members with floating-point scores
-- Perfect for leaderboards, priority queues, time-series indexes
-- ============================================================

-- Create a foreign table backed by a Redis ZSET
CREATE FOREIGN TABLE demo_leaderboard (member text, score numeric)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'zset',
        table_key_prefix 'game:leaderboard'
    );

-- INSERT adds scored members
INSERT INTO demo_leaderboard VALUES ('alice', 2500);
INSERT INTO demo_leaderboard VALUES ('bob', 1800);
INSERT INTO demo_leaderboard VALUES ('charlie', 3200);
INSERT INTO demo_leaderboard VALUES ('diana', 2900);
INSERT INTO demo_leaderboard VALUES ('eve', 1500);

-- SELECT returns members sorted by score (ascending)
SELECT * FROM demo_leaderboard;

-- ORDER BY and LIMIT work for top-N queries
SELECT * FROM demo_leaderboard ORDER BY score DESC LIMIT 3;

-- WHERE pushdown: score range queries
SELECT * FROM demo_leaderboard WHERE score >= 2000;

-- UPDATE a member's score
UPDATE demo_leaderboard SET score = 3500 WHERE member = 'alice';
SELECT * FROM demo_leaderboard ORDER BY score DESC;

-- DELETE a member
DELETE FROM demo_leaderboard WHERE member = 'eve';
SELECT * FROM demo_leaderboard;

-- Cleanup
DROP FOREIGN TABLE demo_leaderboard;
