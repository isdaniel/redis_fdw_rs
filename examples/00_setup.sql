-- ============================================================
-- Redis FDW RS - Setup
-- ============================================================
-- Prerequisites:
--   make setup-redis   (starts Redis on 127.0.0.1:8899)
--   cargo pgrx install --release
--   cargo pgrx run
-- ============================================================

-- 1. Create the extension
CREATE EXTENSION IF NOT EXISTS redis_fdw_rs;

-- 2. Create the Foreign Data Wrapper
CREATE FOREIGN DATA WRAPPER redis_wrapper
    HANDLER redis_fdw_handler
    VALIDATOR redis_fdw_validator;

-- 3. Create the server connection (points to local Redis container)
CREATE SERVER redis_server
    FOREIGN DATA WRAPPER redis_wrapper
    OPTIONS (host_port '127.0.0.1:8899');

-- Verify setup
SELECT * FROM pg_foreign_server WHERE srvname = 'redis_server';
