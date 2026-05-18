# AGENTS.md — Redis FDW RS

## What This Project Is

A **PostgreSQL Foreign Data Wrapper** that maps Redis data structures to SQL tables. Written in Rust using the [pgrx](https://github.com/pgcentralfoundation/pgrx) framework (v0.18.0). Users create foreign tables with options like `table_type 'hash'` and `table_key_prefix 'user:profiles'`, then use standard SQL (SELECT/INSERT/UPDATE/DELETE) to interact with Redis.

## Current State

**Feature-complete for all CRUD operations.** All Redis types (String, Hash, List, Set, ZSet) support SELECT, INSERT, UPDATE, DELETE. Stream supports SELECT, INSERT, DELETE only (append-only by design).

### Recent Work
- TTL support: table-level default + per-row override via virtual `ttl` column
- Multi-key pattern queries: glob patterns in `table_key_prefix` for scanning multiple keys
- DDL-time option validation via `redis_fdw_validator`
- UPDATE support implemented for all types (except Stream)
- Cost estimation for query planner (`src/query/cost_estimation.rs`)
- Connection pooling via R2D2 with global pool manager
- WHERE clause pushdown optimization
- LIMIT/OFFSET handling
- Auto-release GitHub pipeline on `v*` tags

### Known Issues
- Cluster integration tests (9 tests) fail without Redis Cluster infrastructure running
- All non-cluster tests pass (187/187)

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

### Adding a New Feature

1. If it's a new Redis operation, implement it on `RedisTableOperations` trait in `src/tables/interface.rs`
2. Add implementation for each type in `src/tables/implementations/{type}.rs`
3. Add dispatch in `src/tables/types.rs` (use existing macro pattern)
4. Wire the FDW callback in `src/core/handlers.rs`
5. Add state management method in `src/core/state_manager.rs` if needed
6. Add tests in `src/tests/`

### Key Files to Understand First
| File | Purpose |
|------|---------|
| `src/core/handlers.rs` | All PostgreSQL FDW callbacks — the entry point for everything |
| `src/core/validator.rs` | DDL-time option validation (VALIDATOR function) |
| `src/tables/interface.rs` | The `RedisTableOperations` trait — defines what each type must implement |
| `src/tables/types.rs` | `RedisTableType` enum + dispatch methods |
| `src/core/state_manager.rs` | `RedisFdwState` — holds connection, table type, scan state, TTL, multi-key |
| `src/query/pushdown.rs` | WHERE clause analysis and optimization |

### Architecture Diagram
```
PostgreSQL Query
    │
    ▼
FDW Callbacks (handlers.rs)
    │
    ├── Planning: get_foreign_rel_size → get_foreign_paths → get_foreign_plan
    │       └── cost_estimation.rs, pushdown.rs
    │
    ├── Scanning: begin_foreign_scan → iterate_foreign_scan → end_foreign_scan
    │       └── state_manager.rs → RedisTableType dispatch → implementations/*
    │
    └── Modify: begin_foreign_modify → exec_foreign_{insert,update,delete}
            └── state_manager.rs → RedisTableType dispatch → implementations/*
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
| redis | 1.2.1 | Redis client (with cluster, streams, r2d2 features) |
| r2d2 | 0.8.10 | Connection pooling |
| thiserror | 2.0.12 | Error types |
| rand | 0.9.2 | Random generation utilities |
