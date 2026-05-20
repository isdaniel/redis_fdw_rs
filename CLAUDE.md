# Redis FDW RS - Project Instructions

## Project Overview

PostgreSQL Foreign Data Wrapper (FDW) extension written in Rust that exposes Redis data as PostgreSQL tables. Built with **pgrx 0.18.0**, supports PostgreSQL 14-18, Redis standalone and cluster modes.

## Build & Test Commands

```bash
# Build (debug)
cargo build 

# Build (release)
cargo build --release

# Run tests (requires Redis on 127.0.0.1:8899 and pgrx init)
cargo pgrx test pg14

# Run for a specific PG version
cargo pgrx test pg15
cargo pgrx test pg16
cargo pgrx test pg17
cargo pgrx test pg18

# Install extension into PG
cargo pgrx install --release

# Run interactive PG session
cargo pgrx run

# Clippy
cargo clippy --all-targets --features pg14

# Format
cargo fmt
```

## Key Architecture

### Module Structure
- `src/core/` — FDW handler callbacks (`handlers.rs`), state management (`state_manager.rs`), connection pool (`pool_manager.rs`), connection factory (`connection_factory.rs`), DDL validator (`validator.rs`)
- `src/tables/` — Trait interface (`interface.rs`), type enum + dispatch (`types.rs`, `macros.rs`), per-type implementations in `implementations/`
- `src/query/` — WHERE pushdown (`pushdown.rs`), cost estimation (`cost_estimation.rs`), LIMIT handling (`limit.rs`), scan ops (`scan_ops.rs`)
- `src/join/` — JOIN support: parameterized paths (`parameterized.rs`), FDW-to-FDW pushdown (`foreign_join.rs`), types (`types.rs`)
- `src/auth/` — Redis authentication
- `src/utils/` — Cell/Row types, memory context helpers, general utilities

### FDW Lifecycle (PostgreSQL callbacks)
1. **Planning**: `get_foreign_rel_size` → `get_foreign_paths` → `get_foreign_plan`
   - Planning (`get_foreign_rel_size`) connects to Redis for real statistics (DBSIZE, HLEN, etc.)
   - `begin_foreign_scan` reuses the planning-phase connection if available
2. **Scanning**: `begin_foreign_scan` → `iterate_foreign_scan` → `re_scan_foreign_scan` → `end_foreign_scan`
   - `recheck_foreign_scan` (returns true; no lossy filtering)
   - `shutdown_foreign_scan` (early connection release back to pool)
3. **Explain**: `explain_foreign_scan`, `explain_foreign_modify` (EXPLAIN output with server, key, type, pushdown, batch info)
4. **Modify**: `plan_foreign_modify` → `begin_foreign_modify` → `exec_foreign_insert`/`update`/`delete` → `end_foreign_modify`
5. **Batch Insert**: `get_foreign_modify_batch_size` → `exec_foreign_batch_insert` (pipelined multi-row)
6. **COPY FROM / INSERT SELECT**: `begin_foreign_insert` → (reuses `exec_foreign_insert`) → `end_foreign_insert`
7. **Truncate**: `exec_foreign_truncate` (UNLINK for single-key; SCAN+UNLINK for patterns)
8. **Import Schema**: `import_foreign_schema` (SCAN → TYPE → group by prefix → generate DDL)
9. **Analyze**: `analyze_foreign_table` → `acquire_sample_rows` (enables `ANALYZE` for query planning)
10. **Updatability**: `is_foreign_rel_updatable` (bitmask: 28 for all types, 24 for stream)
11. **Join Paths**: `get_foreign_join_paths` (FDW-to-FDW same-server pushdown with pipelined fetch)

### Trait Pattern
All Redis types implement `RedisTableOperations` (in `src/tables/interface.rs`):
- `load_data()`, `get_dataset()`, `data_len()`, `get_row()`
- `insert()`, `delete()`, `update()`
- `supports_pushdown()`

Dispatch from `RedisTableType` enum uses macros in `src/tables/macros.rs`.

### Supported Operations

| Type    | SELECT | INSERT | UPDATE | DELETE | TRUNCATE |
|---------|--------|--------|--------|--------|----------|
| String  | ✅     | ✅     | ✅     | ✅     | ✅       |
| Hash    | ✅     | ✅     | ✅     | ✅     | ✅       |
| List    | ✅     | ✅     | ✅     | ✅     | ✅       |
| Set     | ✅     | ✅     | ✅     | ✅     | ✅       |
| ZSet    | ✅     | ✅     | ✅     | ✅     | ✅       |
| Stream  | ✅     | ✅     | ❌     | ✅     | ✅       |

Stream is append-only; UPDATE returns an error at the trait level and `IsForeignRelUpdatable` omits the UPDATE bit for stream tables.

### TTL Support
- Table option `ttl` sets default key expiration (seconds); -1 = persist
- Optional `ttl bigint` column allows per-row override on INSERT/UPDATE
- On SELECT, the `ttl` column returns remaining seconds (-1 = no expiry, -2 = missing)
- TTL detection: `detect_ttl_column()` in handlers.rs finds column by name "ttl"
- TTL stripping: handlers strip the ttl column from data before delegating to table type impl

### Multi-Key Pattern Mode
- If `table_key_prefix` contains `*`, `?`, or `[`, FDW enters multi-key mode
- Detection: `is_multi_key_pattern()` in state_manager.rs
- Scanning uses top-level `SCAN MATCH pattern` to find keys
- Data stored as flat `DataSet::Filtered(Vec<String>)` with N columns per row
- First column is always the Redis key name
- INSERT routes to specific key (first column); DELETE uses `DEL` on the full key

### JOIN Architecture
- **FDW-to-Local**: Standard nested-loop; PostgreSQL drives outer rows, FDW rescans inner on each iteration
- **FDW-to-FDW**: `GetForeignJoinPaths` detects same-server tables, extracts join columns from restrictlist, creates pushdown join path
- **Join column detection**: Walks `extra.restrictlist` → `RestrictInfo` → `OpExpr` → `Var` nodes to find equality condition columns
- **Join execution**: Fetch both datasets → build HashMap on smaller side → probe with larger
- **Connection lifecycle**: acquired at `get_foreign_rel_size`, reused through execution, released at `shutdown_foreign_scan`

### FDW Validator
- `redis_fdw_validator_wrapper` — raw C function (not `#[pg_extern]`) with `pg_finfo`
- SQL type: `(text[], oid)` — PG passes options as `key=value` text array
- Validates server options (host_port required, cluster_mode boolean)
- Validates table options (table_type, table_key_prefix required; database 0-15; ttl; batch_size 100-100000)

### TLS/SSL Support
- Controlled via URI scheme in `host_port`: `rediss://` enables TLS, `#insecure` fragment skips cert verification
- Uses rustls backend via redis crate features (`tls-rustls`, `tls-rustls-insecure`)
- `build_redis_url()` in pool_manager.rs preserves `rediss://` scheme and `#insecure` fragment
- `apply_to_url()` in auth/mod.rs handles both `redis://` and `rediss://` schemes
- Validator's `is_valid_host_port()` strips scheme and fragment before checking host:port format

## Code Conventions

- Use `pgrx::error!()` for PostgreSQL-level errors (never `panic!`)
- Use `pgrx::log!()` for debug logging (prefixed with `--->`)
- All FDW callback functions are `#[pg_guard] unsafe extern "C-unwind"`
- Prefer `&[String]` for data passing between layers
- Connection is accessed via `PooledConnection` from R2D2 pool manager
- Tests use `#[pg_test]` attribute (runs inside a real PG backend)

## Testing Notes

- Tests require Redis running on `127.0.0.1:8899` (database 15 for most tests)
- Cluster tests require 6-node cluster on ports 7001-7006 (use `docker-compose.cluster-test.yml`)
- Start Redis: `docker run -d --name redis-server -p 8899:6379 redis`
- All tests use separate FDW/server names to avoid conflicts

## CI/CD

- `.github/workflows/ci.yaml` — Build + test on push/PR (PG14-18 matrix)
- `.github/workflows/release.yaml` — Auto-release on `v*` tag push, builds packages for PG14-18
- `.github/workflows/release-apt.yaml` — APT package release on `v*` tag (main branch only), PG14-18 × amd64/arm64

## Common Gotchas

- pgrx version must match exactly: `cargo-pgrx 0.18.0` ↔ `pgrx = "=0.18.0"`
- `IsForeignRelUpdatable` uses CmdType bit positions: CMD_UPDATE=2, CMD_INSERT=3, CMD_DELETE=4 → bitmask is `(1<<CmdType)`
- Memory contexts: FDW state lives in a custom `MemoryContext` created per scan/modify operation
- `PgBox::from_pg()` does NOT take ownership — the memory is still managed by PG
