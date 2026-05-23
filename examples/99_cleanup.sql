-- ============================================================
-- Cleanup: Remove all demo artifacts
-- ============================================================
-- Run this at the end of a demo session to wipe extension state.
-- Redis keys are NOT removed here — use TRUNCATE on individual
-- foreign tables (or FLUSHDB on the Redis side) for that.
-- ============================================================

-- Drop the servers (cascades to all foreign tables)
DROP SERVER IF EXISTS redis_server CASCADE;
DROP SERVER IF EXISTS redis_cluster_server CASCADE;

-- Drop the FDW
DROP FOREIGN DATA WRAPPER IF EXISTS redis_wrapper CASCADE;

-- Drop the extension
DROP EXTENSION IF EXISTS redis_fdw_rs;
