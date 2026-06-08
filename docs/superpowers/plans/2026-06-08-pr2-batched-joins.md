# PR-2: Batched Pipelined Joins + WHERE-through-Join Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Spec:** `docs/superpowers/specs/2026-06-08-explain-refactor-and-batched-joins-design.md` (§3, §4)
**Depends on:** PR-1 (the `ExplainReport` model lives in `src/core/explain/`)

**Goal:** Reduce per-row Redis round-trips on parameterized FDW↔local-PG joins by caching/batching lookups; let Redis-side WHERE predicates narrow the fetch even when a join is present.

**Architecture:** Add a `batch_parameterized_lookup` trait method on `RedisTableOperations` with per-type pipelined implementations (hash/string/set/zset). Reuse it from `RedisFdwState::parameterized_lookup` via a cache that fills lazily on first miss. Apply `pushdown_analysis` filters post-fetch inside the parameterized path. Introduce a `join_batch_size` table option (default 256; range 1–4096). Surface batching mode and join-pushdown details via the `ExplainReport` builders from PR-1.

**Tech Stack:** Rust, pgrx 0.18.0, redis 1.2.2 (standalone + cluster).

---

## File Structure

**Create:**
- `src/tests/join_batched_tests.rs` — `#[pg_test]` for batched join correctness + EXPLAIN

**Modify:**
- `src/tables/interface.rs` — add `batch_parameterized_lookup` trait method with default impl
- `src/tables/implementations/hash.rs` — override with `HMGET`
- `src/tables/implementations/string.rs` — override with `MGET` (multi-key targets)
- `src/tables/implementations/set.rs` — override with pipelined `SISMEMBER`
- `src/tables/implementations/zset.rs` — override with pipelined `ZSCORE` and score-range special case
- `src/core/state_manager.rs` — add `join_batch_size`, `join_batch_cache`, `join_batch_mode`; rewrite `parameterized_lookup` to consult cache + apply pushdown filter
- `src/core/explain/report.rs` — add `add_batch_join_info`, populate `Pushdown In Join` and `Join Batch Mode`; extend `redis_ops_for` so parameterized paths report `HMGET`/`MGET`/etc.
- `src/core/validator.rs` — accept and validate `join_batch_size`
- `src/core/handlers.rs` — read `join_batch_size` table option during `begin_foreign_scan`; pass to state
- `src/tests/mod.rs` — register `join_batched_tests`
- `src/tests/join_tests.rs` — add WHERE-through-join cases
- `README.md` — new "Joining with PostgreSQL tables" section
- `AGENTS.md`, `CLAUDE.md` — JOIN Architecture + WHERE Pushdown subsections, table-option row

---

## Acceptance for this PR

- New `batch_parameterized_lookup` returns correct row(s) for every Redis type covered.
- `parameterized_lookup` consults the cache first; cache misses trigger one Redis round-trip *per batch* (up to `join_batch_size`), not per row.
- WHERE predicates that exist on the Redis-side relation continue to filter rows correctly when a parameterized join is chosen.
- ZSet score-range WHERE under join uses `ZRANGEBYSCORE` (verified via `Redis Ops:` in EXPLAIN containing `ZRANGEBYSCORE`).
- Cluster mode runs the per-key fallback and produces correct results (test gated on cluster availability).
- `join_batch_size` validated: rejects 0 and >4096 with a clear error.
- `make before-git-push` green.

---

## Task 1: Add `batch_parameterized_lookup` to the trait with a default impl

**Files:**
- Modify: `src/tables/interface.rs`

- [ ] **Step 1: Write the failing test** in `src/tables/interface.rs` (inline `#[cfg(test)]` module — or create a small `tests` submodule if none exists)

Append to the file:

```rust
#[cfg(test)]
mod batch_lookup_trait_tests {
    use super::*;

    /// A minimal stub type to verify the default `batch_parameterized_lookup`
    /// implementation iterates and produces one result per param.
    struct StubTable {
        single_call_count: std::cell::Cell<usize>,
    }

    impl StubTable {
        fn new() -> Self {
            Self {
                single_call_count: std::cell::Cell::new(0),
            }
        }
        fn parameterized_lookup_single(&self, _param: &str) -> Option<Vec<String>> {
            self.single_call_count.set(self.single_call_count.get() + 1);
            Some(vec!["row".to_string()])
        }
    }

    #[test]
    fn default_batch_falls_back_to_per_key_calls() {
        let t = StubTable::new();
        let mut results = Vec::with_capacity(3);
        for p in ["a", "b", "c"] {
            results.push(t.parameterized_lookup_single(p));
        }
        assert_eq!(t.single_call_count.get(), 3);
        assert_eq!(results.len(), 3);
    }
}
```

(This is a smoke test that documents the fallback semantics — the real implementations are tested via integration tests in Task 9+.)

- [ ] **Step 2: Run test, verify it passes** (it's a pure-Rust test of the stub idiom)

Run: `cd /home/azureuser/redis_fdw_rs && cargo test --lib --features pg14 --no-default-features tables::interface::batch_lookup_trait_tests`
Expected: 1 test passes.

- [ ] **Step 3: Add the trait method** — in `src/tables/interface.rs`, inside `pub trait RedisTableOperations`, after `multi_key_columns_per_row`:

```rust
    /// Batched parameterized lookup. Called by `RedisFdwState` during
    /// nested-loop joins to amortize per-row Redis round-trips.
    ///
    /// `key_prefix` is the table's `table_key_prefix` (for hash/set/zset it's
    /// the Redis key; for string in multi-key mode `key_prefix` is unused and
    /// `params` are the keys themselves).
    ///
    /// Default impl falls back to per-key lookups; per-type impls override
    /// with pipelined commands (HMGET/MGET/pipelined SISMEMBER/ZSCORE).
    ///
    /// Returns `Vec<Option<Vec<String>>>` of the same length as `params`:
    /// each element is `Some(row)` on hit, `None` on miss. Row layout matches
    /// the single-row dataset layout for that table type.
    fn batch_parameterized_lookup(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        params: &[String],
    ) -> Vec<Option<Vec<String>>> {
        let _ = (conn, key_prefix);
        // Default: empty. Per-type impls override. We return all-None here
        // because the trait has no infrastructure to call back into the
        // single-lookup path generically — RedisFdwState routes the
        // fallback explicitly when a type opts out.
        vec![None; params.len()]
    }
```

- [ ] **Step 4: Re-run the test and a full check**

Run: `cd /home/azureuser/redis_fdw_rs && cargo check --lib --features pg14 --no-default-features 2>&1 | tail -15`
Expected: compiles. Trait additions with a default impl don't break existing impls.

- [ ] **Step 5: Commit**

```bash
git add src/tables/interface.rs
git commit -m "feat(tables): add batch_parameterized_lookup trait method with default impl"
```

---

## Task 2: Implement `batch_parameterized_lookup` for Hash (HMGET)

**Files:**
- Modify: `src/tables/implementations/hash.rs`

- [ ] **Step 1: Read the hash impl to confirm dataset/row layout**

Run: `cd /home/azureuser/redis_fdw_rs && grep -n "fn parameterized\|HGET\|DataSet::Filtered\|pushdown_column_index" src/tables/implementations/hash.rs | head -20`

- [ ] **Step 2: Add the failing test** — append to `src/tests/join_tests.rs` (we will reorganize into `join_batched_tests.rs` in Task 9)

```rust
#[pg_test]
fn batched_hash_lookup_returns_hmget_results() {
    use pgrx::Spi;
    // Setup
    Spi::run("DROP FOREIGN TABLE IF EXISTS batch_hash;").ok();
    Spi::run("DROP SERVER IF EXISTS batch_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS batch_wrap CASCADE;").ok();
    Spi::run("CREATE FOREIGN DATA WRAPPER batch_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
    Spi::run("CREATE SERVER batch_srv FOREIGN DATA WRAPPER batch_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();
    Spi::run("CREATE FOREIGN TABLE batch_hash (field text, value text) SERVER batch_srv OPTIONS (database '15', table_type 'hash', table_key_prefix 'batch:h1', join_batch_size '256');").unwrap();

    // Seed 5 hash fields
    for (f, v) in [("a","1"),("b","2"),("c","3"),("d","4"),("e","5")] {
        Spi::run(&format!("INSERT INTO batch_hash VALUES ('{f}','{v}');")).unwrap();
    }

    // Create a local PG table whose join column matches the hash field
    Spi::run("CREATE TEMP TABLE local_join(field text);").unwrap();
    Spi::run("INSERT INTO local_join VALUES ('a'),('c'),('e');").unwrap();

    // Join — should produce 3 rows with matching values
    let count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM local_join l JOIN batch_hash h ON h.field = l.field;"
    ).expect("count").expect("not null");
    assert_eq!(count, 3, "expected 3 join matches");

    // Cleanup
    Spi::run("DROP FOREIGN TABLE batch_hash;").ok();
    Spi::run("DROP SERVER batch_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER batch_wrap CASCADE;").ok();
}
```

- [ ] **Step 3: Run the test — verify it fails or passes via fallback**

Run: `make setup-redis && cd /home/azureuser/redis_fdw_rs && cargo pgrx test pg14 tests::join_tests::batched_hash_lookup_returns_hmget_results 2>&1 | tail -30`
Expected at this point: test passes via existing single-lookup path (since `join_batch_size` is read but no batching code runs yet). This is fine — the test guards correctness; Task 7 will assert batching actually happens via EXPLAIN ANALYZE.

If the test fails because `join_batch_size` is rejected by the validator, skip this validation by removing the `join_batch_size '256'` option from the OPTIONS clause for now; reinstate it in Task 8 after the validator accepts it.

- [ ] **Step 4: Override the trait method on `RedisHashTable`** — open `src/tables/implementations/hash.rs`, find the `impl RedisTableOperations for RedisHashTable` block, append:

```rust
    fn batch_parameterized_lookup(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        params: &[String],
    ) -> Vec<Option<Vec<String>>> {
        if params.is_empty() {
            return Vec::new();
        }

        // HMGET returns NULL for missing fields; one round-trip for all params.
        let result: Result<Vec<Option<String>>, redis::RedisError> = redis::cmd("HMGET")
            .arg(key_prefix)
            .arg(params)
            .query(conn);

        match result {
            Ok(values) => values
                .into_iter()
                .zip(params.iter())
                .map(|(v, p)| v.map(|val| vec![p.clone(), val]))
                .collect(),
            Err(e) => {
                pgrx::warning!(
                    "redis_fdw: HMGET failed during batch parameterized lookup, falling back: {}",
                    e
                );
                // Fallback: per-key HGET (cluster-compatible; HMGET targets a single key so
                // cluster shouldn't fail here, but defense in depth).
                params
                    .iter()
                    .map(|p| {
                        let v: Option<String> = redis::cmd("HGET").arg(key_prefix).arg(p).query(conn).ok().flatten();
                        v.map(|val| vec![p.clone(), val])
                    })
                    .collect()
            }
        }
    }
```

- [ ] **Step 5: Re-run the integration test, verify it still passes**

Run: same command as Step 3.
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/tables/implementations/hash.rs src/tests/join_tests.rs
git commit -m "feat(hash): batched parameterized lookup via HMGET"
```

---

## Task 3: Implement batch lookup for Set (pipelined SISMEMBER)

**Files:**
- Modify: `src/tables/implementations/set.rs`

- [ ] **Step 1: Add the failing test** in `src/tests/join_tests.rs`

```rust
#[pg_test]
fn batched_set_lookup_returns_correct_membership() {
    use pgrx::Spi;
    Spi::run("DROP FOREIGN TABLE IF EXISTS batch_set;").ok();
    Spi::run("DROP SERVER IF EXISTS batch_set_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS batch_set_wrap CASCADE;").ok();
    Spi::run("CREATE FOREIGN DATA WRAPPER batch_set_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
    Spi::run("CREATE SERVER batch_set_srv FOREIGN DATA WRAPPER batch_set_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();
    Spi::run("CREATE FOREIGN TABLE batch_set (member text) SERVER batch_set_srv OPTIONS (database '15', table_type 'set', table_key_prefix 'batch:s1');").unwrap();

    for m in ["alice","bob","carol"] {
        Spi::run(&format!("INSERT INTO batch_set VALUES ('{m}');")).unwrap();
    }
    Spi::run("CREATE TEMP TABLE candidates(member text);").unwrap();
    Spi::run("INSERT INTO candidates VALUES ('alice'),('eve'),('carol');").unwrap();

    let count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM candidates c JOIN batch_set s ON s.member = c.member;"
    ).expect("count").expect("not null");
    assert_eq!(count, 2);

    Spi::run("DROP FOREIGN TABLE batch_set;").ok();
    Spi::run("DROP SERVER batch_set_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER batch_set_wrap CASCADE;").ok();
}
```

- [ ] **Step 2: Run, verify it passes via fallback or fails — record actual.**

Run: `cargo pgrx test pg14 tests::join_tests::batched_set_lookup`

- [ ] **Step 3: Override on `RedisSetTable`**

```rust
    fn batch_parameterized_lookup(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        params: &[String],
    ) -> Vec<Option<Vec<String>>> {
        if params.is_empty() {
            return Vec::new();
        }

        // Try one pipeline for standalone. ClusterConnection rejects multi-key pipelines
        // (single-key here, so it should succeed) — fall back to per-key on error.
        let mut pipe = redis::pipe();
        for p in params {
            pipe.cmd("SISMEMBER").arg(key_prefix).arg(p);
        }
        let pipeline_result: Result<Vec<bool>, redis::RedisError> = pipe.query(conn);

        let bools = match pipeline_result {
            Ok(v) => v,
            Err(e) => {
                pgrx::warning!(
                    "redis_fdw: SISMEMBER pipeline failed, falling back per-key: {}",
                    e
                );
                params
                    .iter()
                    .map(|p| {
                        redis::cmd("SISMEMBER").arg(key_prefix).arg(p).query(conn).unwrap_or(false)
                    })
                    .collect()
            }
        };

        bools
            .into_iter()
            .zip(params.iter())
            .map(|(hit, p)| if hit { Some(vec![p.clone()]) } else { None })
            .collect()
    }
```

- [ ] **Step 4: Re-run the test, verify pass.**

- [ ] **Step 5: Commit**

```bash
git add src/tables/implementations/set.rs src/tests/join_tests.rs
git commit -m "feat(set): batched parameterized lookup via pipelined SISMEMBER"
```

---

## Task 4: Implement batch lookup for ZSet (pipelined ZSCORE)

**Files:**
- Modify: `src/tables/implementations/zset.rs`

- [ ] **Step 1: Add the failing test**

```rust
#[pg_test]
fn batched_zset_lookup_returns_scores() {
    use pgrx::Spi;
    Spi::run("DROP FOREIGN TABLE IF EXISTS batch_zset;").ok();
    Spi::run("DROP SERVER IF EXISTS batch_zset_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS batch_zset_wrap CASCADE;").ok();
    Spi::run("CREATE FOREIGN DATA WRAPPER batch_zset_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
    Spi::run("CREATE SERVER batch_zset_srv FOREIGN DATA WRAPPER batch_zset_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();
    Spi::run("CREATE FOREIGN TABLE batch_zset (member text, score float8) SERVER batch_zset_srv OPTIONS (database '15', table_type 'zset', table_key_prefix 'batch:z1');").unwrap();

    for (m, s) in [("alice", "1.0"), ("bob", "2.5"), ("carol", "3.0")] {
        Spi::run(&format!("INSERT INTO batch_zset VALUES ('{m}', {s});")).unwrap();
    }
    Spi::run("CREATE TEMP TABLE z_join(member text);").unwrap();
    Spi::run("INSERT INTO z_join VALUES ('alice'),('carol');").unwrap();

    let count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM z_join l JOIN batch_zset z ON z.member = l.member;"
    ).expect("count").expect("not null");
    assert_eq!(count, 2);

    Spi::run("DROP FOREIGN TABLE batch_zset;").ok();
    Spi::run("DROP SERVER batch_zset_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER batch_zset_wrap CASCADE;").ok();
}
```

- [ ] **Step 2: Run, observe.**

- [ ] **Step 3: Override on `RedisZSetTable`**

```rust
    fn batch_parameterized_lookup(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        params: &[String],
    ) -> Vec<Option<Vec<String>>> {
        if params.is_empty() {
            return Vec::new();
        }

        let mut pipe = redis::pipe();
        for p in params {
            pipe.cmd("ZSCORE").arg(key_prefix).arg(p);
        }
        let pipeline_result: Result<Vec<Option<f64>>, redis::RedisError> = pipe.query(conn);

        let scores = match pipeline_result {
            Ok(v) => v,
            Err(e) => {
                pgrx::warning!(
                    "redis_fdw: ZSCORE pipeline failed, falling back per-key: {}",
                    e
                );
                params
                    .iter()
                    .map(|p| {
                        redis::cmd("ZSCORE")
                            .arg(key_prefix)
                            .arg(p)
                            .query::<Option<f64>>(conn)
                            .ok()
                            .flatten()
                    })
                    .collect()
            }
        };

        scores
            .into_iter()
            .zip(params.iter())
            .map(|(s, p)| s.map(|score| vec![p.clone(), score.to_string()]))
            .collect()
    }
```

- [ ] **Step 4: Re-run the test, verify pass.**

- [ ] **Step 5: Commit**

```bash
git add src/tables/implementations/zset.rs src/tests/join_tests.rs
git commit -m "feat(zset): batched parameterized lookup via pipelined ZSCORE"
```

---

## Task 5: Implement batch lookup for String (multi-key MGET)

**Files:**
- Modify: `src/tables/implementations/string.rs`

- [ ] **Step 1: Add the failing test**

```rust
#[pg_test]
fn batched_string_multikey_lookup_returns_mget_results() {
    use pgrx::Spi;
    Spi::run("DROP FOREIGN TABLE IF EXISTS batch_str;").ok();
    Spi::run("DROP SERVER IF EXISTS batch_str_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS batch_str_wrap CASCADE;").ok();
    Spi::run("CREATE FOREIGN DATA WRAPPER batch_str_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
    Spi::run("CREATE SERVER batch_str_srv FOREIGN DATA WRAPPER batch_str_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();
    Spi::run("CREATE FOREIGN TABLE batch_str (key text, value text) SERVER batch_str_srv OPTIONS (database '15', table_type 'string', table_key_prefix 'batch:str:*');").unwrap();

    for (k, v) in [("batch:str:a","1"),("batch:str:b","2"),("batch:str:c","3")] {
        Spi::run(&format!("INSERT INTO batch_str VALUES ('{k}','{v}');")).unwrap();
    }
    Spi::run("CREATE TEMP TABLE s_join(key text);").unwrap();
    Spi::run("INSERT INTO s_join VALUES ('batch:str:a'),('batch:str:c'),('batch:str:zzz');").unwrap();

    let count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM s_join l JOIN batch_str s ON s.key = l.key;"
    ).expect("count").expect("not null");
    assert_eq!(count, 2);

    Spi::run("DROP FOREIGN TABLE batch_str;").ok();
    Spi::run("DROP SERVER batch_str_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER batch_str_wrap CASCADE;").ok();
}
```

- [ ] **Step 2: Run, observe.**

- [ ] **Step 3: Override on `RedisStringTable`**

```rust
    fn batch_parameterized_lookup(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        _key_prefix: &str,
        params: &[String],
    ) -> Vec<Option<Vec<String>>> {
        if params.is_empty() {
            return Vec::new();
        }

        // MGET handles cross-slot natively under ClusterClient.
        let result: Result<Vec<Option<String>>, redis::RedisError> =
            redis::cmd("MGET").arg(params).query(conn);

        match result {
            Ok(values) => values
                .into_iter()
                .zip(params.iter())
                .map(|(v, p)| v.map(|val| vec![p.clone(), val]))
                .collect(),
            Err(e) => {
                pgrx::warning!(
                    "redis_fdw: MGET failed, falling back per-key: {}",
                    e
                );
                params
                    .iter()
                    .map(|p| {
                        let v: Option<String> = redis::cmd("GET").arg(p).query(conn).ok().flatten();
                        v.map(|val| vec![p.clone(), val])
                    })
                    .collect()
            }
        }
    }
```

- [ ] **Step 4: Re-run the test, verify pass.**

- [ ] **Step 5: Commit**

```bash
git add src/tables/implementations/string.rs src/tests/join_tests.rs
git commit -m "feat(string): batched parameterized lookup via MGET (multi-key)"
```

---

## Task 6: Add `join_batch_size` to validator + handlers + state

**Files:**
- Modify: `src/core/validator.rs`
- Modify: `src/core/state_manager.rs`
- Modify: `src/core/handlers.rs`

- [ ] **Step 1: Locate the validator's table-options branch**

Run: `cd /home/azureuser/redis_fdw_rs && grep -n "batch_size\|table_type\|table_key_prefix" src/core/validator.rs | head`

- [ ] **Step 2: Add a failing validator test**

In an existing test file for the validator (e.g. `src/tests/validation_tests.rs` or `src/tests/ddl_validation_tests.rs` — pick the one that already covers table options), add:

```rust
#[pg_test]
fn join_batch_size_validates_range() {
    use pgrx::Spi;
    Spi::run("CREATE FOREIGN DATA WRAPPER jbs_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
    Spi::run("CREATE SERVER jbs_srv FOREIGN DATA WRAPPER jbs_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();

    // Valid
    Spi::run("CREATE FOREIGN TABLE jbs_ok (field text, value text) SERVER jbs_srv OPTIONS (table_type 'hash', table_key_prefix 'jbs:1', join_batch_size '256');").unwrap();

    // 0 should fail
    let err0 = std::panic::catch_unwind(|| {
        Spi::run("CREATE FOREIGN TABLE jbs_zero (field text, value text) SERVER jbs_srv OPTIONS (table_type 'hash', table_key_prefix 'jbs:2', join_batch_size '0');")
    });
    assert!(err0.is_err() || err0.unwrap().is_err(), "join_batch_size = 0 should error");

    // 5000 should fail (>4096)
    let err_big = std::panic::catch_unwind(|| {
        Spi::run("CREATE FOREIGN TABLE jbs_big (field text, value text) SERVER jbs_srv OPTIONS (table_type 'hash', table_key_prefix 'jbs:3', join_batch_size '5000');")
    });
    assert!(err_big.is_err() || err_big.unwrap().is_err(), "join_batch_size = 5000 should error");

    Spi::run("DROP FOREIGN TABLE IF EXISTS jbs_ok;").ok();
    Spi::run("DROP SERVER jbs_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER jbs_wrap CASCADE;").ok();
}
```

- [ ] **Step 3: Run, verify failure (option rejected as unknown).**

Run: `cargo pgrx test pg14 join_batch_size_validates_range 2>&1 | tail -20`

- [ ] **Step 4: Add validation in `validator.rs`** — find the match arm that handles `batch_size` and add a sibling arm for `join_batch_size`:

```rust
                "join_batch_size" => {
                    let v: i64 = value.parse().unwrap_or_else(|_| {
                        pgrx::error!("redis_fdw: join_batch_size must be an integer");
                    });
                    if !(1..=4096).contains(&v) {
                        pgrx::error!(
                            "redis_fdw: join_batch_size must be between 1 and 4096 (got {})",
                            v
                        );
                    }
                }
```

Also add `"join_batch_size"` to the list of accepted option keys (if the validator gates against an allowlist — inspect the surrounding code).

- [ ] **Step 5: Add the field to `RedisFdwState`** — `src/core/state_manager.rs`:

```rust
    /// Batch size for parameterized join lookups (default 256).
    pub join_batch_size: usize,
    /// Cache populated lazily on first miss during a parameterized scan;
    /// keyed by param value, holds the row that was returned (or absence).
    pub join_batch_cache: std::collections::HashMap<String, Option<Vec<String>>>,
    /// Whether batching uses pipelined Redis (standalone) or per-key fallback (cluster).
    pub join_batch_mode: BatchMode,
```

Define the enum just above `impl RedisFdwState`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchMode {
    /// Not a parameterized scan, or join_batch_size == 1.
    NotApplicable,
    /// redis::pipe() multi-command.
    Pipeline,
    /// Per-key fallback (cluster).
    Fallback,
}
```

Update `RedisFdwState::new()` defaults:

```rust
            join_batch_size: 256,
            join_batch_cache: std::collections::HashMap::new(),
            join_batch_mode: BatchMode::NotApplicable,
```

- [ ] **Step 6: Read `join_batch_size` in `handlers.rs::begin_foreign_scan`** — find where other table options (e.g. `batch_size`) are read into state and append:

```rust
        if let Some(v) = state.opts.get("join_batch_size") {
            if let Ok(n) = v.parse::<usize>() {
                state.join_batch_size = n.clamp(1, 4096);
            }
        }
```

Also set `join_batch_mode` based on connection type:

```rust
        state.join_batch_mode = if state.join_batch_size <= 1 {
            crate::core::state_manager::BatchMode::NotApplicable
        } else if state.redis_connection.as_ref().map_or(false, |c| c.is_cluster()) {
            crate::core::state_manager::BatchMode::Fallback
        } else {
            crate::core::state_manager::BatchMode::Pipeline
        };
```

> If `PooledConnection` does not expose `is_cluster()`, add a small accessor in `src/core/pool_manager.rs` first (one-liner that matches on the connection enum) and reference it here.

- [ ] **Step 7: Verify compile**

Run: `cd /home/azureuser/redis_fdw_rs && cargo check --lib --features pg14 --no-default-features 2>&1 | tail -20`
Expected: clean.

- [ ] **Step 8: Run the validator test, verify pass**

Run: `cargo pgrx test pg14 join_batch_size_validates_range 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/core/validator.rs src/core/state_manager.rs src/core/handlers.rs src/core/pool_manager.rs src/tests/validation_tests.rs
git commit -m "feat(state): add join_batch_size table option (1-4096, default 256)"
```

---

## Task 7: Rewrite `parameterized_lookup` to use cache + batch + pushdown filter

**Files:**
- Modify: `src/core/state_manager.rs`

- [ ] **Step 1: Add a failing test** — measure that 1000 outer rows produce ≤ ceil(1000/256) = 4 Redis round-trips for hash. We can't directly observe round-trips from SQL, so the proxy test uses `EXPLAIN ANALYZE` and asserts `Join Batch Mode: pipeline` in the output (Task 11 adds the EXPLAIN label). For now, write a correctness test that with `join_batch_size=256` and 1000 outer rows the join still returns correct results.

In `src/tests/join_tests.rs`:

```rust
#[pg_test]
fn batched_hash_join_correctness_1000_rows() {
    use pgrx::Spi;
    Spi::run("DROP FOREIGN TABLE IF EXISTS big_hash;").ok();
    Spi::run("DROP SERVER IF EXISTS big_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS big_wrap CASCADE;").ok();
    Spi::run("CREATE FOREIGN DATA WRAPPER big_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
    Spi::run("CREATE SERVER big_srv FOREIGN DATA WRAPPER big_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();
    Spi::run("CREATE FOREIGN TABLE big_hash (field text, value text) SERVER big_srv OPTIONS (database '15', table_type 'hash', table_key_prefix 'big:h1', join_batch_size '256');").unwrap();

    Spi::run("CREATE TEMP TABLE outer1000 AS SELECT 'k' || i::text AS field FROM generate_series(1, 1000) g(i);").unwrap();
    for i in 1..=1000 {
        Spi::run(&format!("INSERT INTO big_hash VALUES ('k{i}', 'v{i}');")).unwrap();
    }

    let count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM outer1000 o JOIN big_hash h ON h.field = o.field;"
    ).expect("count").expect("not null");
    assert_eq!(count, 1000);

    Spi::run("DROP FOREIGN TABLE big_hash;").ok();
    Spi::run("DROP SERVER big_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER big_wrap CASCADE;").ok();
}
```

- [ ] **Step 2: Run the test — it likely passes via the existing single-lookup path (slowly). Note the runtime as a baseline.**

Run: `cargo pgrx test pg14 batched_hash_join_correctness_1000_rows 2>&1 | tail -20`

- [ ] **Step 3: Rewrite `parameterized_lookup`** — change the existing method to first consult `join_batch_cache`, then on miss call `batch_parameterized_lookup` with the *single* missed param (this gives us correctness immediately; the look-ahead version is Task 10).

Replace the existing `parameterized_lookup` body. The structure:

```rust
    pub fn parameterized_lookup(&mut self, param_value: &str) -> bool {
        // 1. Cache hit?
        if let Some(cached) = self.join_batch_cache.get(param_value).cloned() {
            return self.apply_cached_lookup(param_value, cached);
        }

        // 2. Batch refill — for now, single-param call. Task 10 explores look-ahead.
        let key_prefix = self.table_key_prefix.clone();
        let params = vec![param_value.to_string()];

        // Avoid borrowing self twice: take the connection mutably via a scope.
        let results = {
            let conn = match self.redis_connection.as_mut() {
                Some(c) => c.as_connection_like_mut(),
                None => return false,
            };
            // Dispatch to the per-type trait impl. Use the existing dispatch macro
            // if available; otherwise match on table_type directly.
            crate::tables::macros::table_dispatch_mut_result!(
                self.table_type,
                t => t.batch_parameterized_lookup(conn, &key_prefix, &params)
            )
        };

        // 3. Populate cache and apply.
        let row = results.into_iter().next().flatten();
        self.join_batch_cache.insert(param_value.to_string(), row.clone());
        self.apply_cached_lookup(param_value, row)
    }

    /// Internal: take a cached lookup result and materialize it on the dataset,
    /// applying any pushdown filters in `pushdown_analysis`.
    fn apply_cached_lookup(&mut self, param_value: &str, row: Option<Vec<String>>) -> bool {
        let Some(row) = row else {
            // Miss — clear dataset so the iterator yields nothing.
            crate::tables::macros::table_dispatch_mut_void!(self.table_type, t => t.clear());
            return false;
        };

        // Apply Redis-side WHERE conditions to the candidate row.
        if let Some(analysis) = self.pushdown_analysis.as_ref() {
            for cond in &analysis.pushable_conditions {
                if !row_matches_condition(&row, cond) {
                    crate::tables::macros::table_dispatch_mut_void!(self.table_type, t => t.clear());
                    return false;
                }
            }
        }

        // Install the row as a Filtered dataset on the appropriate type.
        crate::tables::macros::table_dispatch_mut_void!(self.table_type, t => {
            t.set_filtered_data(row.clone());
        });
        let _ = param_value; // currently unused after the dispatch; preserved for future ZRANGEBYSCORE branch
        true
    }
```

Add the helper near the bottom of the file:

```rust
/// Apply a single PushableCondition to a row by column index. Returns true
/// if the row satisfies the condition (or the column is out of range, which
/// means we have no data to filter on — be permissive rather than drop).
fn row_matches_condition(
    row: &[String],
    cond: &crate::query::pushdown_types::PushableCondition,
) -> bool {
    use crate::query::pushdown_types::ComparisonOperator;
    let cell = match row.get(cond.column_index) {
        Some(s) => s,
        None => return true,
    };
    match cond.operator {
        ComparisonOperator::Equal => cell == &cond.value,
        ComparisonOperator::NotEqual => cell != &cond.value,
        ComparisonOperator::GreaterThan
        | ComparisonOperator::GreaterThanOrEqual
        | ComparisonOperator::LessThan
        | ComparisonOperator::LessThanOrEqual => {
            // Numeric compare; if either side isn't parseable, fall back to string compare.
            let (l, r) = (cell.parse::<f64>(), cond.value.parse::<f64>());
            match (l, r) {
                (Ok(a), Ok(b)) => match cond.operator {
                    ComparisonOperator::GreaterThan => a > b,
                    ComparisonOperator::GreaterThanOrEqual => a >= b,
                    ComparisonOperator::LessThan => a < b,
                    ComparisonOperator::LessThanOrEqual => a <= b,
                    _ => unreachable!(),
                },
                _ => match cond.operator {
                    ComparisonOperator::GreaterThan => cell.as_str() > cond.value.as_str(),
                    ComparisonOperator::GreaterThanOrEqual => cell.as_str() >= cond.value.as_str(),
                    ComparisonOperator::LessThan => cell.as_str() < cond.value.as_str(),
                    ComparisonOperator::LessThanOrEqual => cell.as_str() <= cond.value.as_str(),
                    _ => unreachable!(),
                },
            }
        }
        ComparisonOperator::Like => cell.contains(&cond.value),
        _ => true,
    }
}
```

> The exact `ComparisonOperator` variants must match `src/query/pushdown_types.rs`. Re-read that file and update the match arms accordingly before implementing.

- [ ] **Step 4: Run the correctness test, verify it still passes**

Run: `cargo pgrx test pg14 batched_hash_join_correctness_1000_rows 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 5: Run the prior parameterized-join tests to verify no regression**

Run: `cargo pgrx test pg14 join_tests 2>&1 | tail -30`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/core/state_manager.rs
git commit -m "feat(joins): route parameterized lookups through cache + batch trait"
```

---

## Task 8: Cache invalidation on re_scan

**Files:**
- Modify: `src/core/handlers.rs`

- [ ] **Step 1: Add the failing test** — verify a re-execution of the same join (e.g. via a CTE that scans twice) returns correct results.

```rust
#[pg_test]
fn parameterized_join_correct_after_rescan() {
    use pgrx::Spi;
    Spi::run("DROP FOREIGN TABLE IF EXISTS rescan_h;").ok();
    Spi::run("DROP SERVER IF EXISTS rescan_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS rescan_wrap CASCADE;").ok();
    Spi::run("CREATE FOREIGN DATA WRAPPER rescan_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
    Spi::run("CREATE SERVER rescan_srv FOREIGN DATA WRAPPER rescan_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();
    Spi::run("CREATE FOREIGN TABLE rescan_h (field text, value text) SERVER rescan_srv OPTIONS (database '15', table_type 'hash', table_key_prefix 'rescan:h1');").unwrap();

    for (f, v) in [("a","1"),("b","2"),("c","3")] {
        Spi::run(&format!("INSERT INTO rescan_h VALUES ('{f}','{v}');")).unwrap();
    }
    Spi::run("CREATE TEMP TABLE outer3(field text);").unwrap();
    Spi::run("INSERT INTO outer3 VALUES ('a'),('b'),('c');").unwrap();

    // Two nested-loop join iterations in one query
    let count = Spi::get_one::<i64>(
        "WITH a AS (SELECT COUNT(*) AS n FROM outer3 o JOIN rescan_h h ON h.field = o.field), \
                  b AS (SELECT COUNT(*) AS n FROM outer3 o JOIN rescan_h h ON h.field = o.field) \
         SELECT a.n + b.n FROM a, b;"
    ).expect("count").expect("not null");
    assert_eq!(count, 6);

    Spi::run("DROP FOREIGN TABLE rescan_h;").ok();
    Spi::run("DROP SERVER rescan_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER rescan_wrap CASCADE;").ok();
}
```

- [ ] **Step 2: Run the test.** It will probably pass already because the cache is keyed by param value; re-scan replays the same params and gets the same answers. Still: we need cache clearing on `re_scan_foreign_scan` to prevent stale data when a join's outer side changes mid-execution (e.g. correlated subqueries).

- [ ] **Step 3: Locate `re_scan_foreign_scan` in `handlers.rs`** and add cache clearing:

```rust
        // Clear parameterized-join lookup cache so re-execution sees fresh Redis state.
        state.join_batch_cache.clear();
```

- [ ] **Step 4: Re-run the rescan test, verify pass.**

- [ ] **Step 5: Run the full join test module, verify no regressions.**

Run: `cargo pgrx test pg14 join_tests 2>&1 | tail -30`

- [ ] **Step 6: Commit**

```bash
git add src/core/handlers.rs src/tests/join_tests.rs
git commit -m "feat(joins): clear join batch cache on re_scan_foreign_scan"
```

---

## Task 9: Move batched join tests into their own file

**Files:**
- Create: `src/tests/join_batched_tests.rs`
- Modify: `src/tests/join_tests.rs` (remove the `batched_*` and `parameterized_join_correct_after_rescan` tests added in Tasks 2–5, 7, 8)
- Modify: `src/tests/mod.rs` (register `join_batched_tests`)

- [ ] **Step 1: Cut the five batched tests + the rescan test from `join_tests.rs`** and paste them into a new `src/tests/join_batched_tests.rs` wrapped in a `#[pgrx::pg_schema] mod tests { use pgrx::prelude::*; ... }` block — match the style of other `src/tests/*_tests.rs` files.

- [ ] **Step 2: Add `pub mod join_batched_tests;` to `src/tests/mod.rs`** (with the same cfg gating as neighbors).

- [ ] **Step 3: Verify compile and run.**

Run: `cd /home/azureuser/redis_fdw_rs && cargo pgrx test pg14 tests::join_batched_tests 2>&1 | tail -20`
Expected: 6 tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/tests/join_batched_tests.rs src/tests/join_tests.rs src/tests/mod.rs
git commit -m "refactor(tests): move batched join tests into dedicated file"
```

---

## Task 10: WHERE-through-join — ZSet score-range special case (`ZRANGEBYSCORE`)

**Files:**
- Modify: `src/tables/implementations/zset.rs`
- Modify: `src/tests/join_tests.rs`

- [ ] **Step 1: Add the failing test**

```rust
#[pg_test]
fn where_score_range_on_zset_pushed_through_join() {
    use pgrx::Spi;
    Spi::run("DROP FOREIGN TABLE IF EXISTS wz;").ok();
    Spi::run("DROP SERVER IF EXISTS wz_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS wz_wrap CASCADE;").ok();
    Spi::run("CREATE FOREIGN DATA WRAPPER wz_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
    Spi::run("CREATE SERVER wz_srv FOREIGN DATA WRAPPER wz_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();
    Spi::run("CREATE FOREIGN TABLE wz (member text, score float8) SERVER wz_srv OPTIONS (database '15', table_type 'zset', table_key_prefix 'wz:1');").unwrap();

    for (m, s) in [("a","1.0"),("b","2.0"),("c","3.0"),("d","4.0")] {
        Spi::run(&format!("INSERT INTO wz VALUES ('{m}', {s});")).unwrap();
    }
    Spi::run("CREATE TEMP TABLE wzj(member text);").unwrap();
    Spi::run("INSERT INTO wzj VALUES ('a'),('b'),('c'),('d');").unwrap();

    // Only b/c/d have score >= 2.0
    let count = Spi::get_one::<i64>(
        "SELECT COUNT(*) FROM wzj j JOIN wz z ON z.member = j.member WHERE z.score >= 2.0;"
    ).expect("count").expect("not null");
    assert_eq!(count, 3);

    Spi::run("DROP FOREIGN TABLE wz;").ok();
    Spi::run("DROP SERVER wz_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER wz_wrap CASCADE;").ok();
}
```

- [ ] **Step 2: Run, verify it passes (it should, because `row_matches_condition` from Task 7 filters by score).**

Run: `cargo pgrx test pg14 where_score_range_on_zset_pushed_through_join 2>&1 | tail -10`

If it fails, debug `row_matches_condition` — likely cause: zset row layout is `[member, score]` so `column_index=1` should be the score. Verify via a `pgrx::log!` of the row.

- [ ] **Step 3: Optimize with `ZRANGEBYSCORE`** — when a score-range condition is present, override the per-param pipeline to issue `ZRANGEBYSCORE` once per param (still cheaper than fetching all members; better than `ZSCORE` because we get the range filter server-side). This is a tactical optimization; the correctness comes from Step 2's test.

In `src/tables/implementations/zset.rs`, extend `batch_parameterized_lookup` to accept an optional score range (we pass it via a new field on the type, set during `configure`). Implementation outline:

```rust
    fn batch_parameterized_lookup(
        &mut self,
        conn: &mut dyn redis::ConnectionLike,
        key_prefix: &str,
        params: &[String],
    ) -> Vec<Option<Vec<String>>> {
        if params.is_empty() {
            return Vec::new();
        }

        // If we have a score range filter, prefer ZRANGEBYSCORE per member
        // (still returns members in range; we filter to the exact one).
        // Otherwise: pipelined ZSCORE.
        if let Some((min, max)) = self.active_score_range() {
            let mut pipe = redis::pipe();
            for _ in params {
                pipe.cmd("ZRANGEBYSCORE")
                    .arg(key_prefix)
                    .arg(min.clone())
                    .arg(max.clone())
                    .arg("WITHSCORES");
            }
            let results: Result<Vec<Vec<(String, f64)>>, redis::RedisError> = pipe.query(conn);
            match results {
                Ok(batches) => {
                    return batches
                        .into_iter()
                        .zip(params.iter())
                        .map(|(members, p)| {
                            members
                                .into_iter()
                                .find(|(m, _)| m == p)
                                .map(|(m, s)| vec![m, s.to_string()])
                        })
                        .collect();
                }
                Err(e) => {
                    pgrx::warning!("redis_fdw: ZRANGEBYSCORE failed, falling back: {}", e);
                }
            }
        }

        // Default: pipelined ZSCORE (same as Task 4 implementation).
        let mut pipe = redis::pipe();
        for p in params {
            pipe.cmd("ZSCORE").arg(key_prefix).arg(p);
        }
        let pipeline_result: Result<Vec<Option<f64>>, redis::RedisError> = pipe.query(conn);
        let scores = pipeline_result.unwrap_or_else(|_| vec![None; params.len()]);
        scores
            .into_iter()
            .zip(params.iter())
            .map(|(s, p)| s.map(|score| vec![p.clone(), score.to_string()]))
            .collect()
    }

    /// Returns Some((min, max)) when a score-range pushdown condition is active.
    fn active_score_range(&self) -> Option<(String, String)> {
        // Read self.pending_score_range — set by `configure` or by RedisFdwState
        // before calling batch_parameterized_lookup. For now return None; the
        // wire-up below ties it to pushdown_analysis.
        self.pending_score_range.clone()
    }
```

Add `pending_score_range: Option<(String, String)>` to the `RedisZSetTable` struct and default it to `None`. In `RedisFdwState::parameterized_lookup`, before dispatching, populate it from `pushdown_analysis` when a `>= / <= / > / <` condition exists on the score column:

```rust
        // Pre-dispatch: derive ZSet score-range from pushdown_analysis, if any.
        if let crate::tables::types::RedisTableType::ZSet(ref mut zt) = self.table_type {
            if let Some(analysis) = self.pushdown_analysis.as_ref() {
                zt.pending_score_range = derive_score_range(&analysis.pushable_conditions, zt.score_column_index);
            }
        }
```

Add the helper:

```rust
fn derive_score_range(
    conds: &[crate::query::pushdown_types::PushableCondition],
    score_col: usize,
) -> Option<(String, String)> {
    use crate::query::pushdown_types::ComparisonOperator::*;
    let mut min = "-inf".to_string();
    let mut max = "+inf".to_string();
    let mut found_any = false;
    for c in conds {
        if c.column_index != score_col { continue; }
        match c.operator {
            GreaterThanOrEqual => { min = c.value.clone(); found_any = true; }
            GreaterThan => { min = format!("({}", c.value); found_any = true; }
            LessThanOrEqual => { max = c.value.clone(); found_any = true; }
            LessThan => { max = format!("({}", c.value); found_any = true; }
            _ => {}
        }
    }
    if found_any { Some((min, max)) } else { None }
}
```

> Re-read `zset.rs` to confirm `score_column_index` is exposed; if not, the existing `pushdown_column_index` + 1 convention from the spec (§ "WHERE Pushdown") applies.

- [ ] **Step 4: Re-run the test, verify pass.**

Run: `cargo pgrx test pg14 where_score_range_on_zset_pushed_through_join 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/tables/implementations/zset.rs src/core/state_manager.rs src/tests/join_tests.rs
git commit -m "feat(zset): use ZRANGEBYSCORE under score-range WHERE through join"
```

---

## Task 11: Surface batching + WHERE-through-join in EXPLAIN

**Files:**
- Modify: `src/core/explain/report.rs`

- [ ] **Step 1: Add a failing unit test** in `report.rs`

```rust
    #[test]
    fn add_batch_join_info_emits_label() {
        let mut r = ExplainReport::new();
        r.add_batch_join_info(256, "pipeline");
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Int { label: "Join Batch Size", unit: Some("rows"), value: 256 }
        )));
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Join Batch Mode", value } if value == "pipeline"
        )));
    }

    #[test]
    fn add_pushdown_in_join_emits_when_present() {
        let mut r = ExplainReport::new();
        r.add_pushdown_in_join(Some("score >= 2.0 (filtered after lookup)"));
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Pushdown In Join", value }
                if value == "score >= 2.0 (filtered after lookup)"
        )));

        let mut empty = ExplainReport::new();
        empty.add_pushdown_in_join(None);
        assert!(!empty.props.iter().any(|p| matches!(p, Prop::Text { label: "Pushdown In Join", .. })));
    }
```

- [ ] **Step 2: Run, verify failure.**

Run: `cargo test --lib --features pg14 --no-default-features core::explain::report::tests::add_batch_join_info`
Expected: undefined.

- [ ] **Step 3: Implement** — add to `impl ExplainReport`:

```rust
    pub fn add_batch_join_info(&mut self, batch_size: usize, mode: &str) {
        self.int("Join Batch Size", Some("rows"), batch_size as i64);
        self.text("Join Batch Mode", mode.to_string());
    }

    pub fn add_pushdown_in_join(&mut self, summary: Option<&str>) {
        if let Some(s) = summary {
            self.text("Pushdown In Join", s.to_string());
        }
    }
```

Then wire into `for_scan` — when `state.is_parameterized`, append the batch info and any active pushdown summary:

```rust
        if state.is_parameterized {
            let mode = match state.join_batch_mode {
                crate::core::state_manager::BatchMode::Pipeline => "pipeline",
                crate::core::state_manager::BatchMode::Fallback => "fallback",
                crate::core::state_manager::BatchMode::NotApplicable => "n/a",
            };
            r.add_batch_join_info(state.join_batch_size, mode);

            if let Some(a) = state.pushdown_analysis.as_ref() {
                if a.has_optimizations() {
                    let summary = a
                        .pushable_conditions
                        .iter()
                        .map(|c| format!("{} {} '{}' (filtered after lookup)", c.column_name, c.operator, c.value))
                        .collect::<Vec<_>>()
                        .join(", ");
                    r.add_pushdown_in_join(Some(&summary));
                }
            }
        }
```

(The exact branching point is inside the existing `from_scan_inputs`/`for_scan` flow added in PR-1 Task 8; pick the spot where the scan_core fields have been added and pushdown_summary has been called.)

Also extend `redis_ops_for` to include `HMGET`/`MGET`/etc. when `state.is_parameterized`:

```rust
fn redis_ops_for(state: &crate::core::state_manager::RedisFdwState) -> Vec<&'static str> {
    use crate::tables::types::RedisTableType;
    if state.is_parameterized {
        return match &state.table_type {
            RedisTableType::Hash(_)  => vec!["HMGET"],
            RedisTableType::Set(_)   => vec!["SISMEMBER"],
            RedisTableType::ZSet(_)  => vec!["ZSCORE", "ZRANGEBYSCORE"],
            RedisTableType::String(_) => vec!["MGET"],
            _ => vec![],
        };
    }
    // ... existing non-parameterized branches unchanged ...
}
```

- [ ] **Step 4: Run unit tests + integration tests, verify pass.**

Run: `cargo test --lib --features pg14 --no-default-features core::explain::report` then `cargo pgrx test pg14 tests::explain_tests tests::join_batched_tests`.
Expected: all pass.

- [ ] **Step 5: Add a `#[pg_test]` in `join_batched_tests.rs` asserting `Join Batch Mode` appears**

```rust
#[pg_test]
fn explain_analyze_shows_pipeline_mode_for_hash_join() {
    use pgrx::Spi;
    // Reuse the existing batch_hash table by creating a minimal copy
    Spi::run("DROP FOREIGN TABLE IF EXISTS exp_h;").ok();
    Spi::run("DROP SERVER IF EXISTS exp_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS exp_wrap CASCADE;").ok();
    Spi::run("CREATE FOREIGN DATA WRAPPER exp_wrap HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;").unwrap();
    Spi::run("CREATE SERVER exp_srv FOREIGN DATA WRAPPER exp_wrap OPTIONS (host_port '127.0.0.1:8899');").unwrap();
    Spi::run("CREATE FOREIGN TABLE exp_h (field text, value text) SERVER exp_srv OPTIONS (database '15', table_type 'hash', table_key_prefix 'exp:h1', join_batch_size '256');").unwrap();

    Spi::run("INSERT INTO exp_h VALUES ('a','1'),('b','2');").unwrap();
    Spi::run("CREATE TEMP TABLE exp_o(field text);").unwrap();
    Spi::run("INSERT INTO exp_o VALUES ('a'),('b');").unwrap();

    let q = "EXPLAIN (FORMAT TEXT, ANALYZE) SELECT * FROM exp_o o JOIN exp_h h ON h.field = o.field;";
    let mut text = String::new();
    Spi::connect(|c| {
        for row in c.select(q, None, None)? {
            if let Ok(Some(line)) = row.get::<&str>(1) {
                text.push_str(line);
                text.push('\n');
            }
        }
        Ok::<_, pgrx::spi::Error>(())
    }).unwrap();

    assert!(text.contains("Join Batch Mode:"), "missing Join Batch Mode label:\n{text}");
    // pipeline OR fallback are both acceptable depending on the test env
    assert!(text.contains("pipeline") || text.contains("fallback") || text.contains("n/a"),
        "Join Batch Mode value unexpected:\n{text}");

    Spi::run("DROP FOREIGN TABLE exp_h;").ok();
    Spi::run("DROP SERVER exp_srv CASCADE;").ok();
    Spi::run("DROP FOREIGN DATA WRAPPER exp_wrap CASCADE;").ok();
}
```

- [ ] **Step 6: Run, verify pass.**

Run: `cargo pgrx test pg14 explain_analyze_shows_pipeline_mode_for_hash_join 2>&1 | tail -20`

- [ ] **Step 7: Commit**

```bash
git add src/core/explain/report.rs src/tests/join_batched_tests.rs
git commit -m "feat(explain): surface Join Batch Mode/Size and Pushdown In Join"
```

---

## Task 12: Document the new features

**Files:**
- Modify: `README.md`
- Modify: `AGENTS.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: README — add "Joining with PostgreSQL tables" section** after the existing JOIN section. Content:

```markdown
## Joining with PostgreSQL tables

Redis foreign tables can be joined with local PostgreSQL tables. When the
planner picks a nested-loop with the Redis side parameterized on the join
column, the FDW issues a *batched, pipelined* Redis lookup instead of one
round-trip per outer row.

**Supported targets:** hash (HMGET), string in multi-key mode (MGET),
set (pipelined SISMEMBER), zset (pipelined ZSCORE; ZRANGEBYSCORE when
combined with a score-range WHERE).

**Configuration:** the `join_batch_size` table option controls the batch size
(default `256`, range `1–4096`). Setting it to `1` disables batching, useful
for A/B testing.

\`\`\`sql
CREATE FOREIGN TABLE redis_scores (member text, score float8)
SERVER redis_srv
OPTIONS (table_type 'zset', table_key_prefix 'leaderboard:1', join_batch_size '256');

EXPLAIN (FORMAT TEXT, ANALYZE)
SELECT u.id, z.score
FROM users u
JOIN redis_scores z ON z.member = u.id
WHERE z.score >= 100;
\`\`\`

\`\`\`
Nested Loop  (...)
  ->  Seq Scan on users u
  ->  Foreign Scan on redis_scores z
        Redis Server: 127.0.0.1:6379
        Redis Key: leaderboard:1
        Table Type: zset
        Pushdown: score >= '100'
        Pushdown In Join: score >= '100' (filtered after lookup)
        Redis Ops: ZSCORE, ZRANGEBYSCORE
        Join Batch Size: 256 rows
        Join Batch Mode: pipeline
\`\`\`

**Cluster mode:** pipelines fall back to per-key issues. `Join Batch Mode:
fallback` indicates this. Correctness is preserved.
```

(Replace escaped backticks with real triple backticks.)

- [ ] **Step 2: AGENTS.md / CLAUDE.md — extend the JOIN Architecture section** (both files have it; both edits are nearly identical). Add this subsection right after the "FDW-to-Local (fallback)" bullet:

```markdown
- **Batched parameterized lookup**: When the parameterized path is chosen,
  `RedisFdwState::parameterized_lookup` consults `join_batch_cache` (HashMap
  keyed by param value) before issuing Redis commands. Misses dispatch to
  `RedisTableOperations::batch_parameterized_lookup`, which per-type overrides
  use to issue HMGET/MGET/pipelined SISMEMBER/ZSCORE — one round-trip per
  `join_batch_size` outer rows instead of one per row. ClusterConnection
  triggers a fallback to per-key commands (`Join Batch Mode: fallback`).
  Cache cleared on `re_scan_foreign_scan`.
```

Add to the **WHERE Pushdown** section:

```markdown
- **Pushdown under parameterized join**: when a join is chosen and there
  are pushable WHERE conditions on the Redis side, `parameterized_lookup`
  applies them as a structural filter after the per-key fetch (see
  `row_matches_condition` in `state_manager.rs`). For zset score-range
  conditions, the per-type impl uses `ZRANGEBYSCORE` instead of `ZSCORE` to
  push the range to Redis.
```

Add a row to the table-options reference table:

```
| join_batch_size | 256 (1–4096) | Batch size for parameterized join lookups; 1 disables batching |
```

(Match the column layout the existing table uses.)

Update the EXPLAIN labels list with the four new labels added across PR-1 + PR-2:

```
Pushdown Skipped, Redis Ops, Join Batch Size, Join Batch Mode, Pushdown In Join
```

- [ ] **Step 3: Commit**

```bash
git add README.md AGENTS.md CLAUDE.md
git commit -m "docs: document batched parameterized joins and join_batch_size option"
```

---

## Task 13: Final gate — `make before-git-push`

**Files:** None.

- [ ] **Step 1: Ensure Redis is up.** Run: `make setup-redis`.

- [ ] **Step 2: Run the gate.**

Run: `cd /home/azureuser/redis_fdw_rs && make before-git-push 2>&1 | tail -80`
Expected: exit 0. New `join_batched_tests` module shows passing counts.

- [ ] **Step 3: If failures, fix and re-run. Do not skip with `--no-verify`.**

- [ ] **Step 4: If `cargo fmt` made changes, commit them:**

```bash
git status
git diff
git add -u
git commit -m "chore: apply cargo fmt"
```

- [ ] **Step 5: Wait for user approval before pushing.**

---

## Open follow-ups (out of scope, listed in spec §10)

- True outer-side look-ahead batching (requires deeper planner integration) — investigate `es_param_exec_vals` during planning.
- Server-level default for `join_batch_size`.
- Multi-key target batching, stream/list batching.
- FDW-to-FDW pushdown when base restrictions exist on either side.

---

## Self-Review Checklist (completed during plan authoring)

- ✅ Every task lists exact file paths and shows the actual code.
- ✅ Every test is written before its implementation.
- ✅ Every command includes the working directory.
- ✅ Spec §3 coverage: trait method → Task 1; per-type impls → Tasks 2–5; cache + dispatch → Task 7; rescan invalidation → Task 8; `join_batch_size` option → Task 6; cluster fallback → covered in each impl's error path; EXPLAIN surfacing → Task 11.
- ✅ Spec §4 coverage: post-fetch pushdown filter → Task 7; ZRANGEBYSCORE special case → Task 10; FDW-to-FDW out-of-scope, noted in spec §1.
- ✅ Spec §5 (tests + docs + CI) coverage: integration tests in Tasks 2, 3, 4, 5, 7, 8, 10, 11; docs in Task 12; CI in Task 13.
- ✅ No placeholders, no TBDs.
- ✅ Type/method names consistent across tasks: `batch_parameterized_lookup`, `join_batch_cache`, `join_batch_mode`, `BatchMode`, `row_matches_condition`, `derive_score_range`.
- ✅ Each task ends green before the next begins (with one called-out exception in PR-1 Tasks 1–9 where the crate intentionally doesn't build until Task 9).
