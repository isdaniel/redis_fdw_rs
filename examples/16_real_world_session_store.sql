-- ============================================================
-- Demo 16: Real-World Use Case - Session Store
-- ============================================================
-- Common pattern: using Redis FDW as a session/cache layer
-- that can be queried with standard SQL alongside local tables
-- ============================================================

-- Create local user table (simulating your app's database)
CREATE TEMPORARY TABLE app_users (
    user_id text PRIMARY KEY,
    username text,
    email text,
    created_at timestamp DEFAULT now()
);

INSERT INTO app_users VALUES
    ('u:1001', 'alice', 'alice@corp.com'),
    ('u:1002', 'bob', 'bob@corp.com'),
    ('u:1003', 'charlie', 'charlie@corp.com');

-- Redis-backed session store (Hash per session)
CREATE FOREIGN TABLE session_store (field text, value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'hash',
        table_key_prefix 'session:active'
    );

-- Simulate active sessions
INSERT INTO session_store VALUES ('u:1001', '{"ip":"10.0.0.1","last_seen":"2024-01-15T10:30:00Z"}');
INSERT INTO session_store VALUES ('u:1003', '{"ip":"10.0.0.5","last_seen":"2024-01-15T10:45:00Z"}');

-- Redis-backed feature flags (Set)
CREATE FOREIGN TABLE feature_flags (member text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'set',
        table_key_prefix 'features:beta_users'
    );

INSERT INTO feature_flags VALUES ('u:1001');
INSERT INTO feature_flags VALUES ('u:1002');

-- ============================================================
-- Query: Find active users with their session data
-- ============================================================
SELECT
    u.username,
    u.email,
    s.value AS session_data
FROM app_users u
JOIN session_store s ON u.user_id = s.field;

-- ============================================================
-- Query: Find beta users who are currently online
-- ============================================================
SELECT
    u.username,
    u.email
FROM app_users u
JOIN feature_flags f ON u.user_id = f.member
JOIN session_store s ON u.user_id = s.field;

-- ============================================================
-- Query: All users with their online status
-- ============================================================
SELECT
    u.username,
    u.email,
    CASE WHEN s.value IS NOT NULL THEN 'online' ELSE 'offline' END AS status
FROM app_users u
LEFT JOIN session_store s ON u.user_id = s.field;

-- Cleanup
DROP TABLE app_users;
DROP FOREIGN TABLE session_store;
DROP FOREIGN TABLE feature_flags;
