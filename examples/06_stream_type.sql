-- ============================================================
-- Demo 6: Stream Type - Append-Only Event Log
-- ============================================================
-- Redis Stream: append-only log with auto-generated IDs
-- Perfect for event sourcing, audit logs, message queues
-- Note: Streams are INSERT-only (no UPDATE)
-- ============================================================

-- Create a foreign table backed by a Redis STREAM
CREATE FOREIGN TABLE demo_audit_log (aaa text, user_id text, action text, resource text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'stream',
        table_key_prefix 'audit:events'
    );

-- INSERT appends events (id='*' for auto-generated timestamp ID)
INSERT INTO demo_audit_log VALUES ('*', 'user:alice', 'CREATE', 'project:alpha');
INSERT INTO demo_audit_log VALUES ('*', 'user:bob', 'UPDATE', 'project:alpha');
INSERT INTO demo_audit_log VALUES ('*', 'user:alice', 'DELETE', 'file:readme.md');
INSERT INTO demo_audit_log VALUES ('*', 'user:charlie', 'CREATE', 'project:beta');

-- SELECT reads the stream
SELECT * FROM demo_audit_log;

-- Filter by field values
SELECT * FROM demo_audit_log WHERE user_id = 'user:alice';
SELECT * FROM demo_audit_log WHERE action = 'CREATE';

-- Cleanup
DROP FOREIGN TABLE demo_audit_log;
