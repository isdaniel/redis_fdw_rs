# AGENTS.md — Redis FDW RS

## What This Project Is

A **PostgreSQL Foreign Data Wrapper** that maps Redis data structures to SQL tables. Written in Rust using the [pgrx](https://github.com/pgcentralfoundation/pgrx) framework (v0.18.0). Users create foreign tables with options like `table_type 'hash'` and `table_key_prefix 'user:profiles'`, then use standard SQL (SELECT/INSERT/UPDATE/DELETE) to interact with Redis.

## Current State

**Feature-complete for all CRUD operations plus EXPLAIN, batch INSERT, TRUNCATE, IMPORT FOREIGN SCHEMA, ANALYZE, and COPY FROM.** All Redis types (String, Hash, List, Set, ZSet) support SELECT, INSERT, UPDATE, DELETE, TRUNCATE. Stream supports SELECT, INSERT, DELETE, TRUNCATE only (append-only by design).

### Recent Work
- Cluster multi-key pipeline fallback: `load_multi_key_data()` for hash/set/zset/list now uses "try pipeline, fall back to individual commands" pattern for Redis Cluster compatibility (cluster `ClusterConnection` doesn't support `redis::pipe()`)
- Cluster TTL pipeline fallback: `fetch_multi_key_optimized()` TTL batch in `state_manager.rs` uses same try-pipe-then-individual pattern — TTL values now show real remaining seconds in cluster mode (previously returned -2)
- SmallVec optimization: fallback paths use `SmallVec<[T; 8]>` (or `SmallVec<[i64; 64]>` for TTL) to avoid heap allocation for typical small key sets from pushdown queries
- Multi-key pushdown optimization: WHERE conditions on the key column (`=`, `IN`, `LIKE`) bypass full SCAN — direct GET for `=`, pipelined batch for `IN`, narrowed `SCAN MATCH` for `LIKE`
- `compute_key_column_index()` in `column_utils.rs`: determines key column position accounting for TTL column placement
- TTL-position-aware modify path: `add_foreign_update_targets` now skips TTL column at position 0, fixing DELETE/UPDATE crashes on TTL-first tables
- TTL-aware parameterized JOIN paths: `add_parameterized_paths` uses `compute_pushdown_column_index()` instead of hardcoded column 0
- TTL-aware FDW-to-FDW join columns: `get_foreign_join_paths` adjusts join column indices for TTL stripping via `adjust_column_for_ttl_strip()`
- Single-key String join guard: FDW-to-FDW pushdown correctly rejects single-key String tables (no join column to match)
- ZSet score-range pushdown: `WHERE score >= X` / `score <= Y` uses ZRANGEBYSCORE instead of full ZSCAN (O(log N + M) vs O(N))
- DDL-time column validation: `object_access_hook` in `ddl_hook.rs` validates column count at `CREATE FOREIGN TABLE` time (no longer deferred to first query)
- Position-based column filtering: `PushableCondition.column_index` replaces hardcoded column name checks in hash/zset pushdown
- Column validation: `validate_column_count()` enforces per-type column constraints at first query time (string=1, hash=2, list=1-2, set=1, zset=2, stream=2+)
- Parameterized JOIN paths: `get_foreign_paths` advertises O(1) point-lookup paths for FDW-to-local JOINs (HGET, SISMEMBER, ZSCORE)
- JOIN support: FDW-to-FDW join pushdown for same-server tables with automatic join column detection from query clauses
- Connection pool optimization: planning phase now connects to Redis for real cardinality statistics
- FDW lifecycle refactoring: batch insert logic moved to `state_manager.batch_insert_data()`; handlers is now a thin dispatch layer
- ANALYZE support: `analyze_foreign_table` + `acquire_sample_rows` for query planning statistics
- COPY FROM / INSERT SELECT: `begin_foreign_insert` / `end_foreign_insert` callbacks
- ShutdownForeignScan: early connection release back to R2D2 pool for better concurrency
- RecheckForeignScan: correctness for join rechecks (returns true unconditionally)
- EXPLAIN support: `explain_foreign_scan` and `explain_foreign_modify` callbacks with server, key, type, pushdown, batch info
- Batch INSERT: `exec_foreign_batch_insert` with pipelined Redis commands and configurable `batch_size`
- TRUNCATE: `exec_foreign_truncate` using UNLINK (single-key) or SCAN+UNLINK (multi-key patterns)
- IMPORT FOREIGN SCHEMA: `import_foreign_schema` auto-discovers keys, groups by prefix, generates DDL
- OOM robustness: soft limits with warnings for large datasets (100K per-key, 1M total), pool saturation cap (64 pools)
- TTL support: table-level default + per-row override via virtual `ttl` column
- Multi-key pattern queries: glob patterns in `table_key_prefix` for scanning multiple keys
- DDL-time option validation via `redis_fdw_validator`
- DDL-time column count validation via `object_access_hook` (rejects invalid CREATE FOREIGN TABLE)
- TLS/SSL support: `rediss://` URI scheme for encrypted connections (rustls backend)
- UPDATE support implemented for all types (except Stream)
- Cost estimation for query planner (`src/query/cost_estimation.rs`)
- Connection pooling via R2D2 with global pool manager
- WHERE clause pushdown optimization (position-aware: handles TTL column at any position, multi-key offset)
- LIMIT/OFFSET handling
- Auto-release GitHub pipeline on `v*` tags

### Known Issues
- Cluster mode: `SCAN`-based operations (full multi-key scan without key pushdown, `LIKE` pushdown, `TRUNCATE` on patterns) are not yet supported in cluster mode — requires per-node SCAN iteration
- Cluster mode: List type multi-key INSERT misroutes the key column (treats key as element) — separate fix needed
- Cluster integration tests (9 tests) require Redis Cluster infrastructure on ports 7001-7006
- All non-cluster tests pass (including 28 JOIN tests, 16 column validation tests, and 4 pool performance tests)

## How to Work on This Project

### Prerequisites
```bash
# Redis on port 8899
docker run -d --name redis-server -p 8899:6379 redis

# pgrx toolchain
cargo install --locked cargo-pgrx --version 0.18.0
cargo pgrx init --pg14=/usr/lib/postgresql/14/bin/pg_config
```

### Build & Test
```bash
cargo build                    # compile
cargo pgrx test pg14           # run all tests (needs Redis)
cargo clippy --features pg14   # lint
cargo fmt                      # format
```

### Testing Multi-Key Pushdown

```bash
# Run multi-key pushdown tests
cargo pgrx test pg16 multi_key_pushdown_tests
```

Multi-key pushdown tests verify:
- `=` on key column → direct key lookup (no SCAN)
- `IN` on key column → batch pipeline fetch
- `LIKE` on key column → narrowed SCAN MATCH pattern
- All Redis types (String, Hash, Set, ZSet)
- TTL column at any position (start, end)
- Non-existent key returns 0 rows

### Testing JOINs

JOIN tests require both Redis and local PostgreSQL tables:

```bash
# Start Redis infrastructure
make setup-redis

# Run join-specific tests
cargo pgrx test pg16 join_tests

# Run column validation tests
cargo pgrx test pg16 column_validation_tests

# Run pool performance tests
cargo pgrx test pg16 pool_performance
```

JOIN tests create temporary local tables + Redis foreign tables and verify:
- FDW-to-local INNER/LEFT JOINs return correct row counts
- FDW-to-local parameterized path point-lookups (HGET/SISMEMBER/ZSCORE)
- FDW-to-FDW same-server joins work with automatic column detection
- Cross-type FDW-to-FDW joins (e.g., hash.field = zset.member)
- JOIN + WHERE pushdown combinations
- NULL padding for unmatched LEFT JOIN rows
- Empty table edge cases
- List type JOINs and String multi-key JOINs
- Large dataset JOINs (100+ rows)
- Rescan correctness (duplicate local rows trigger multiple scans)
- TTL column at position 0 (before data columns) for hash, zset, set JOINs
- TTL column at middle position (between data columns) for hash JOINs
- FDW-to-FDW hash join with TTL at position 0 on both sides

**Join pushdown eligibility requires ALL of:**
1. Both tables on same Redis server (host_port match)
2. Neither table in multi-key pattern mode (no glob in table_key_prefix)
3. Neither table is a Stream type (variable-width rows not supported)
4. Neither table is a single-key String type (only one value, no join column)
5. Equality operator in join condition (`op_mergejoinable()` check)
6. INNER JOIN or LEFT JOIN only (RIGHT/FULL not pushed down)
7. Neither relation has base WHERE restrictions (`baserestrictinfo` must be empty)

### Adding a New Feature

1. If it's a new Redis operation, implement it on `RedisTableOperations` trait in `src/tables/interface.rs`
2. Add implementation for each type in `src/tables/implementations/{type}.rs`
3. Add dispatch in `src/tables/types.rs` (use `table_dispatch!`, `table_dispatch_mut_result!`, or `table_dispatch_mut_void!` macros from `src/tables/macros.rs`)
4. Wire the FDW callback in `src/core/handlers.rs` (registration) — implementation logic goes in the appropriate submodule (`join_handlers.rs`, `explain.rs`, `schema_import.rs`, `truncate.rs`)
5. Shared column/TTL utilities go in `src/core/column_utils.rs`
6. Add state management method in `src/core/state_manager.rs` if needed
7. Add tests in `src/tests/`

### Key Files to Understand First
| File | Purpose |
|------|---------|
| `src/core/handlers.rs` | FDW callback registration + core scan/modify flow |
| `src/core/join_handlers.rs` | Join pushdown: parameterized paths, FDW-to-FDW join planning/execution |
| `src/core/explain.rs` | EXPLAIN output for scan and modify operations |
| `src/core/schema_import.rs` | IMPORT FOREIGN SCHEMA + ANALYZE + acquire_sample_rows |
| `src/core/truncate.rs` | TRUNCATE implementation (UNLINK / SCAN+UNLINK) |
| `src/core/column_utils.rs` | Shared utilities: TTL detection, column validation, data transformation |
| `src/core/validator.rs` | DDL-time option validation (VALIDATOR function) |
| `src/core/ddl_hook.rs` | DDL-time column count validation via `object_access_hook` |
| `src/tables/interface.rs` | The `RedisTableOperations` trait — defines what each type must implement |
| `src/tables/types.rs` | `RedisTableType` enum + dispatch methods |
| `src/tables/macros.rs` | Dispatch macros: `table_dispatch!`, `table_dispatch_mut_result!`, `table_dispatch_mut_void!` |
| `src/core/state_manager.rs` | `RedisFdwState` — holds connection, table type, scan state, TTL, multi-key |
| `src/query/pushdown.rs` | WHERE clause analysis and optimization |

### Architecture Diagram
```
PostgreSQL Query
    │
    ▼
FDW Callbacks (handlers.rs — registration + core scan/modify)
    │
    ├── Planning: get_foreign_rel_size → get_foreign_paths → get_foreign_plan
    │       └── cost_estimation.rs, pushdown.rs
    │
    ├── Scanning: begin_foreign_scan → iterate → recheck → shutdown → end_foreign_scan
    │       └── state_manager.rs → RedisTableType dispatch → implementations/*
    │
    ├── Explain (explain.rs): explain_foreign_scan, explain_foreign_modify
    │       └── Reports server, key, type, pushdown, batch size, rows fetched
    │
    ├── Modify: begin_foreign_modify → exec_foreign_{insert,update,delete}
    │       └── state_manager.rs → RedisTableType dispatch → implementations/*
    │
    ├── COPY FROM / INSERT SELECT: begin_foreign_insert → exec_foreign_insert → end_foreign_insert
    │       └── Standalone state init for bulk insert paths
    │
    ├── Batch Insert: get_foreign_modify_batch_size → exec_foreign_batch_insert
    │       └── state_manager.batch_insert_data() — pipelines rows into Redis
    │
    ├── Truncate (truncate.rs): exec_foreign_truncate
    │       └── UNLINK (single-key) or SCAN+UNLINK (multi-key pattern)
    │
    ├── Import Schema (schema_import.rs): import_foreign_schema
    │       └── SCAN → TYPE pipeline → group by prefix → generate DDL
    │
    ├── Analyze (schema_import.rs): analyze_foreign_table → acquire_sample_rows
    │       └── Enables ANALYZE for query planning (HLEN/SCARD/ZCARD/XLEN + sampling)
    │
    ├── Join Pushdown (join_handlers.rs): get_foreign_join_paths → plan_foreign_join → begin_foreign_join_scan
    │       └── Same-server detection → hash-join execution (build/probe) → iterate results
    │
    └── Column Utilities (column_utils.rs): TTL detection, column validation, data transformation
            │
            ▼
      Redis (via R2D2 pool_manager.rs)
```

## Conventions

- **No panics in FDW callbacks** — use `pgrx::error!()` which does a PostgreSQL longjmp
- **Tests**: `#[pg_test]` for integration tests that need PG; regular `#[test]` for pure Rust unit tests
- **Logging**: `pgrx::log!("---> function_name")` at entry of each callback
- **Error handling**: Return `Result<(), redis::RedisError>` from trait methods; convert to `pgrx::error!()` at the handler level
- **Data flow**: All data between PG and Redis passes as `Vec<String>` or `&[String]`
- **Connection**: Never create raw connections — always go through pool manager

## Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| pgrx | =0.18.0 | PostgreSQL extension framework |
| redis | 1.2.1 | Redis client (with cluster, streams, r2d2, tls-rustls features) |
| r2d2 | 0.8.10 | Connection pooling |
| thiserror | 2.0.12 | Error types |
| rand | 0.9.2 | Random generation utilities |
