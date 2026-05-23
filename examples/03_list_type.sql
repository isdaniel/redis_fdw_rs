-- ============================================================
-- Demo 3: List Type - Ordered Collections
-- ============================================================
-- Redis List: ordered sequence with index-based access
-- Mapped to (index, value) columns
-- ============================================================

-- Create a foreign table backed by a Redis LIST
CREATE FOREIGN TABLE demo_task_queue (index int, value text)
    SERVER redis_server
    OPTIONS (
        database '0',
        table_type 'list',
        table_key_prefix 'queue:tasks'
    );

-- INSERT appends elements to the list
INSERT INTO demo_task_queue (value) VALUES ('deploy-service-a');
INSERT INTO demo_task_queue (value) VALUES ('run-migrations');
INSERT INTO demo_task_queue (value) VALUES ('notify-slack');
INSERT INTO demo_task_queue (value) VALUES ('update-dashboard');

-- SELECT shows all elements with their indices
SELECT * FROM demo_task_queue;

-- Use LIMIT/OFFSET for pagination
SELECT * FROM demo_task_queue LIMIT 2;
SELECT * FROM demo_task_queue LIMIT 2 OFFSET 2;

-- DELETE by value
DELETE FROM demo_task_queue WHERE value = 'run-migrations';
SELECT * FROM demo_task_queue;

-- Cleanup
DROP FOREIGN TABLE demo_task_queue;
