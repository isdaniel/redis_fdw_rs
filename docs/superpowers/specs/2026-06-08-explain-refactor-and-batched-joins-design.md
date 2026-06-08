# Design: EXPLAIN Refactor + Batched Pipelined Redis↔PostgreSQL Joins

**Date:** 2026-06-08
**Status:** Approved (brainstorming) → ready for implementation plan
**Scope:** `redis_fdw_rs` (PostgreSQL FDW on pgrx 0.18.0)

---

## 1. Goals & Non-Goals

### Goals

1. Refactor EXPLAIN output into a pure-Rust `ExplainReport` model with a thin `emit()` adapter. Single, testable place for EXPLAIN logic.
2. Add **batched pipelined nested-loop join** for FDW↔local-PG joins on single-key Redis targets (hash, string, set, zset).
3. Allow **Redis-side WHERE predicates to narrow the fetch even when a join is present** — today the parameterized path bypasses `pushdown_analysis`.
4. Sync `README.md`, `AGENTS.md`, `CLAUDE.md` to reflect new behavior. Add integration tests. `make before-git-push` green.

### Non-goals (this round)

- Multi-key target batching.
- Stream / list batched lookups.
- Deriving Redis filters from the local-PG side's WHERE.
- Cluster-mode pipelined joins beyond per-key fallback (matches existing pattern).
- Cross-server FDW-to-FDW joins.
- Plan-stable EXPLAIN snapshot tests in the regress harness (use `#[pg_test]` text assertions instead).

---

## 2. Phase 1 — EXPLAIN Refactor

### Motivation

Today `src/core/explain.rs` (122 lines) mixes:
- state inspection,
- pushdown description formatting,
- join description formatting,
- `CString` wrapping + raw `pg_sys::ExplainPropertyText` calls.

The logic is hard to unit-test (requires a PG backend), hard to extend (every new EXPLAIN line means another `CString::new(...).unwrap_or_default()` block), and silent on important things (why pushdown was skipped, which Redis ops will run).

### New module layout

```
src/core/explain/
  mod.rs       # #[pg_guard] handlers; re-exports
  report.rs    # ExplainReport + Prop enum — pure Rust, no pg_sys
  emit.rs      # unsafe ExplainReport::emit(*mut pg_sys::ExplainState)
```

`src/core/explain.rs` is replaced by `src/core/explain/mod.rs`.

### Core types (pure Rust)

```rust
pub enum Prop {
    Text { label: &'static str, value: String },
    Int  { label: &'static str, unit: Option<&'static str>, value: i64 },
}

#[derive(Default)]
pub struct ExplainReport {
    pub props: Vec<Prop>,
}

impl ExplainReport {
    pub fn for_scan(state: &RedisFdwState, analyze: bool) -> Self;
    pub fn for_modify(state: &RedisFdwState) -> Self;

    // Internal builders
    fn add_scan_core(&mut self, s: &RedisFdwState);
    fn add_join(&mut self, s: &RedisFdwState);
    fn add_pushdown(&mut self, a: Option<&PushdownAnalysis>);
    fn add_pushdown_skip_reason(&mut self, s: &RedisFdwState);
    fn add_batch_join_info(&mut self, s: &RedisFdwState);  // populated in Phase 2
    fn add_redis_ops(&mut self, s: &RedisFdwState);

    fn text(&mut self, label: &'static str, value: impl Into<String>);
    fn int(&mut self, label: &'static str, unit: Option<&'static str>, value: i64);
}
```

### Handler glue (`mod.rs`)

```rust
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn explain_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
    es: *mut pg_sys::ExplainState,
) {
    let fdw_state = (*node).fdw_state as *mut RedisFdwState;
    if fdw_state.is_null() { return; }
    let state = &*fdw_state;
    ExplainReport::for_scan(state, (*es).analyze).emit(es);
}
```

`explain_foreign_modify` is structurally identical.

### EXPLAIN labels (final set)

| Label | Type | Notes |
|---|---|---|
| `Redis Server` | Text | unchanged |
| `Redis Key` | Text | unchanged |
| `Table Type` | Text | unchanged |
| `Multi-Key Mode` | Text | unchanged |
| `Pushdown` | Text | normalized: `col op 'val'` joined by `, ` |
| `Pushdown Skipped` | Text | **new** — surfaces *why* (e.g. `baserestrictinfo present on inner rel`) |
| `Redis Ops` | Text | **new** — comma-separated ops we'll issue (`HMGET, ZRANGEBYSCORE`) |
| `Batch Size` | Int (rows) | unchanged |
| `Join` | Text | unchanged (`hash(prefix) x set(prefix)`) |
| `Join Batch Mode` | Text | **new in Phase 2** (`pipeline` / `fallback` / `n/a`) |
| `Pushdown In Join` | Text | **new in Phase 2** |
| `Rows Fetched` | Int (rows) | only when `analyze` |

### Testability

Pure-Rust unit tests in `report.rs`:
- `for_scan_includes_server_and_key`
- `for_scan_includes_pushdown_when_present`
- `for_scan_emits_skip_reason_when_pushdown_blocked`
- `for_scan_includes_rows_fetched_only_when_analyze`
- `for_modify_omits_pushdown`
- `for_join_replaces_scan_core`
- `for_scan_emits_redis_ops`

These run on `cargo test` without a PG backend (fixtures construct `RedisFdwState` via a `#[cfg(test)]` `Default`-style builder, or by mocking just the fields read by the report builder; we'll choose the lighter path during implementation).

End-to-end: `src/tests/explain_tests.rs` (`#[pg_test]`) runs real `EXPLAIN (FORMAT TEXT)` and asserts label presence.

---

## 3. Phase 2 — Batched Pipelined Parameterized Joins

### Current behavior

`get_foreign_paths` adds a parameterized path with `PARAMETERIZED_LOOKUP_COST = 0.5` for hash/set/zset. Nested-loop execution drives `re_scan_foreign_scan` per outer row → `iterate_foreign_scan` → `state.parameterized_lookup(&param_value)` → **one Redis round-trip per outer row**.

For 10K outer rows that's 10K RTTs. On a 1 ms-RTT link the join takes 10 s of network alone.

### New behavior

PostgreSQL still calls `iterate_foreign_scan` per outer row, but the FDW *buffers* lookups internally:

1. First call in a parameterized scan: buffer `param_value`, return nothing yet.
2. When buffer hits `join_batch_size` (default 256) OR the outer side signals end (`re_scan` with no param / EOF), flush as one `redis::pipe()`.
3. Cache results by param value in `join_batch_cache: HashMap<String, JoinRow>`.
4. Subsequent `iterate_foreign_scan` calls answer from the cache.

**This is not how nested-loop calls FDWs by default.** The clean way is: register a `ForeignAsync` / async-aware path *or* let the planner think it's still one-row-at-a-time but pre-fetch lazily on first call. We choose the lazy pre-fetch approach because it works under standard nested-loop without changing planner contracts:

- On the first `iterate_foreign_scan` after `re_scan`, peek the outer relation's expected param using the saved `param_info`; we cannot peek upcoming outer tuples without API additions, so **batching here means caching repeats**, not look-ahead.
- True look-ahead batching requires the planner to materialize the outer side. We'll add a `BatchedNestedLoop` opt-in: when `join_batch_size > 1` and the parameterized path is chosen, FDW returns the entire outer-key set in one go by accepting a `Datum[]` of param values via a new pseudo-param mechanism.

**Decision for spec:** start with the simpler caching-only batching (no look-ahead). If profiling under integration tests shows we're still RTT-bound, escalate to the materialized-outer approach in a follow-up. This keeps Phase 2 scoped.

> Open question to resolve during planning: whether pgrx 0.18 exposes enough of `ForeignScanState->ss.ps.state->es_param_exec_vals` to do look-ahead cleanly. If yes, do look-ahead. If no, ship caching-only and note the follow-up.

### State additions (`RedisFdwState`)

```rust
join_batch_size: usize,                       // table option, default 256
join_batch_cache: HashMap<String, JoinRow>,   // param_value → row(s)
join_batch_mode: BatchMode,                   // Pipeline | Fallback | NotApplicable
```

`BatchMode` is set during `begin_foreign_scan` based on connection type (standalone = Pipeline, cluster = Fallback) and surfaced in EXPLAIN.

### Trait method on `RedisTableOperations`

```rust
fn batch_parameterized_lookup(
    &self,
    conn: &mut dyn ConnectionLike,
    params: &[String],
) -> Vec<Option<JoinRow>> {
    // Default: loop calling the single-key path. Each type overrides with a pipeline.
    params.iter()
          .map(|p| self.parameterized_lookup_single(conn, p))
          .collect()
}
```

### Per-type pipelined ops

| Redis type | Single lookup | Batched (standalone) | Cluster fallback |
|---|---|---|---|
| String | `GET key` | `MGET k1 k2 …` | per-key `GET` |
| Hash   | `HGET prefix field` | `HMGET prefix f1 f2 …` | per-field `HGET` (already one prefix, so no slot issue) |
| Set    | `SISMEMBER prefix m` | `redis::pipe().sismember()` × N | per-member `SISMEMBER` |
| ZSet   | `ZSCORE prefix m` | `redis::pipe().zscore()` × N | per-member `ZSCORE` |

For zset score-range under join (see §4), use `ZRANGEBYSCORE` instead of `ZSCORE` when the pushdown analysis carries a score-range condition.

### Cluster mode

Standalone uses `redis::pipe()`. `ClusterConnection` returns an error on pipelined multi-slot commands → catch, fall back to per-key issue (matches `load_multi_key_data` pattern). `Join Batch Mode: fallback` shown in EXPLAIN.

### Configuration

New table option `join_batch_size`:
- Default: 256
- Range: 1–4096 (1 = batching disabled, useful for A/B testing)
- Validated in `validator.rs`

`AGENTS.md` and `CLAUDE.md` Table Options section gets a new row.

---

## 4. Phase 2 — Redis-side WHERE Narrows Fetch Under Join

### Current gap

The parameterized lookup path in `handlers.rs:514` calls `state.parameterized_lookup(&param_value)` directly. It does NOT consult `state.pushdown_analysis`, so a query like:

```sql
SELECT * FROM users u JOIN redis_scores z ON u.id = z.member WHERE z.score >= 100;
```

fetches every matched `z` row from Redis and lets PostgreSQL filter `score >= 100` post-scan, even though we could call `ZRANGEBYSCORE` directly.

### Change

In `parameterized_lookup` and the new `batch_parameterized_lookup`:

1. After computing the candidate `JoinRow`(s), apply `pushdown_analysis.pushable_conditions` as a structural filter (matches `column_index` against row positions — cheap; conditions are already analyzed).
2. For zset, when a score-range condition is present AND the param is a member, prefer `ZRANGEBYSCORE prefix min max` filtered by member match over `ZSCORE prefix member` post-filter. Decided per-call inside the impl, recorded in `Redis Ops` for EXPLAIN.

### FDW-to-FDW path

Out of scope this round. The existing `baserestrictinfo` guard in `planner.rs:464-471` stays. EXPLAIN gains `Pushdown Skipped: baserestrictinfo present on <outer|inner> rel` so users understand why FDW-to-FDW pushdown didn't fire.

---

## 5. Tests

### Unit tests (no Redis, no PG backend)

`src/core/explain/report.rs` (`#[cfg(test)] mod tests`):
- `for_scan_includes_server_and_key`
- `for_scan_includes_pushdown_when_present`
- `for_scan_emits_skip_reason_when_pushdown_blocked`
- `for_scan_includes_rows_fetched_only_when_analyze`
- `for_modify_omits_pushdown`
- `for_join_replaces_scan_core`
- `for_scan_emits_redis_ops_for_each_type`
- `for_scan_emits_batch_mode`

### Integration tests (`#[pg_test]`, Redis on 127.0.0.1:8899)

New files:

- `src/tests/explain_tests.rs`
  - `explain_text_contains_pushdown_for_hash_field_eq`
  - `explain_text_contains_skip_reason_when_join_blocked`
  - `explain_text_contains_redis_ops`
  - `explain_text_contains_batch_size`

- `src/tests/join_batched_tests.rs`
  - `batched_hash_lookup_joins_correctly_1k_rows`
  - `batched_string_mget_joins_correctly`
  - `batched_set_sismember_joins_correctly`
  - `batched_zset_zscore_joins_correctly`
  - `batch_size_1_disables_batching`
  - `explain_analyze_shows_pipeline_mode`
  - `cluster_fallback_path_correct_results` (gated on cluster availability)

- Extend `src/tests/join_tests.rs`:
  - `where_score_range_on_zset_pushed_through_join`
  - `where_field_eq_on_hash_pushed_through_join`
  - `where_pushdown_in_join_visible_in_explain`

### Performance smoke (informational, not gating)

Manual `EXPLAIN ANALYZE` numbers captured in PR-2 description showing batched vs `join_batch_size=1` on a 10K-outer-row join — expect ≥10× speedup on hash/string targets over a network with ≥1ms RTT.

---

## 6. Documentation Updates

### `README.md`

Add **"Joining with PostgreSQL tables"** section after the existing JOIN section:
- Example: PG `users` table joined to Redis `hash` table on user id.
- Show `EXPLAIN ANALYZE` snippet with `Join Batch Mode: pipeline`, `Pushdown In Join: score >= 100`.
- Note `join_batch_size` table option, default 256.
- Performance note: ≥10× over per-row RTT for selective joins.

### `AGENTS.md` and `CLAUDE.md`

Both already document architecture; sync:

- "Module Structure" → add `src/core/explain/` (report.rs, emit.rs, mod.rs).
- "JOIN Architecture" → add subsection "Batched parameterized lookup" with the trait method, the BatchMode enum, the cache lifecycle (built first iteration, freed at `shutdown_foreign_scan` alongside other join memory).
- "WHERE Pushdown" → add subsection "Pushdown under parameterized join" with the post-fetch filter behavior and the zset score-range special case.
- Table options table → add `join_batch_size`.
- EXPLAIN labels → add the four new labels.

---

## 7. Acceptance Criteria

- [ ] `make before-git-push` green on `PG=pg14` (default).
- [ ] All new unit tests pass under `cargo test`.
- [ ] All new `#[pg_test]` tests pass against Redis on `127.0.0.1:8899`.
- [ ] Existing test suite unaffected (no regressions).
- [ ] `cargo clippy --all-targets --features pg14` clean.
- [ ] `cargo fmt --check` clean.
- [ ] README + AGENTS + CLAUDE reflect the new behavior.
- [ ] PR-1 and PR-2 each pass CI standalone (PR-2 rebased on PR-1).

---

## 8. Delivery Plan

**Two PRs off `main`:**

1. **PR-1 — EXPLAIN refactor** (~600 LOC)
   - `src/core/explain/{mod,report,emit}.rs`
   - Remove `src/core/explain.rs`
   - New `Pushdown Skipped` and `Redis Ops` labels (Phase 1 subset).
   - Unit tests + `explain_tests.rs`.
   - Docs: AGENTS/CLAUDE module-structure update, README EXPLAIN labels section.

2. **PR-2 — Batched joins + WHERE-through-join** (~1000 LOC), rebased on PR-1
   - `batch_parameterized_lookup` trait method + per-type impls.
   - `RedisFdwState` additions (`join_batch_size`, cache, mode).
   - `join_batch_size` table option + validator.
   - WHERE filter inside parameterized path; zset score-range special case.
   - EXPLAIN gains `Join Batch Mode` + `Pushdown In Join` (Phase 2 subset).
   - `join_batched_tests.rs` + extensions to `join_tests.rs`.
   - Docs: AGENTS/CLAUDE JOIN-Architecture + WHERE-Pushdown subsections, README "Joining with PostgreSQL tables" section.

---

## 9. Risks & Mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| pgrx 0.18 doesn't expose enough planner state for true look-ahead batching. | Medium | Spec already commits to caching-only batching first; look-ahead is a documented follow-up. |
| Cluster pipeline silently misbehaves (cross-slot) instead of erroring. | Low | Catch all error variants AND assert results length matches param length; if mismatch, fall back. |
| EXPLAIN text assertions are fragile across PG versions. | Medium | Assert *label presence* (e.g. `"Pushdown:"`) not full lines. Document the contract in `explain_tests.rs`. |
| Behavior change in `parameterized_lookup` regresses an existing test. | Medium | PR-2 keeps the old single-lookup code path; new code routes through `batch_parameterized_lookup(&[one_param])` only when `join_batch_size > 1`. |
| Memory growth from `join_batch_cache` on large joins. | Low | Cache cleared in `shutdown_foreign_scan` alongside existing join memory cleanup; bounded by `join_batch_size` between flushes. |

---

## 10. Open Questions (resolved during planning, not now)

1. Does pgrx 0.18 expose `es_param_exec_vals` for look-ahead? — Plan-time research.
2. Should `join_batch_size` be a *server* option (global default) in addition to a *table* option? — Decide based on user feedback after PR-2.
3. Do we want a `pgrx::warning!` when `join_batch_size=1` is detected on a parameterized path? — Decide during implementation; probably yes.
