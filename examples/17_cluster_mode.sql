-- ============================================================
-- Demo 17: Cluster Mode
-- ============================================================
-- Redis FDW supports Redis Cluster (sharded multi-node).
-- The only difference is adding cluster_mode='true' to the server.
-- All operations work the same way.
--
-- Prerequisites: make setup-redis (starts cluster on ports 7001-7006)
-- ============================================================

-- Create a cluster server connection
CREATE SERVER redis_cluster_server
    FOREIGN DATA WRAPPER redis_wrapper
    OPTIONS (
        host_port '127.0.0.1:7001',
        cluster_mode 'true'
    );

-- Hash table on the cluster
CREATE FOREIGN TABLE demo_cluster_hash (field text, value text)
    SERVER redis_cluster_server
    OPTIONS (
        table_type 'hash',
        table_key_prefix 'cluster:config'
    );

-- Operations work identically to standalone
INSERT INTO demo_cluster_hash VALUES ('version', '3.2.1');
INSERT INTO demo_cluster_hash VALUES ('env', 'production');
INSERT INTO demo_cluster_hash VALUES ('region', 'us-west-2');

SELECT * FROM demo_cluster_hash;
SELECT value FROM demo_cluster_hash WHERE field = 'env';

-- Set table on the cluster
CREATE FOREIGN TABLE demo_cluster_set (member text)
    SERVER redis_cluster_server
    OPTIONS (
        table_type 'set',
        table_key_prefix 'cluster:nodes:active'
    );

INSERT INTO demo_cluster_set VALUES ('node-1.internal');
INSERT INTO demo_cluster_set VALUES ('node-2.internal');
INSERT INTO demo_cluster_set VALUES ('node-3.internal');

SELECT * FROM demo_cluster_set;

-- Cleanup
DROP FOREIGN TABLE demo_cluster_hash;
DROP FOREIGN TABLE demo_cluster_set;
DROP SERVER redis_cluster_server;
