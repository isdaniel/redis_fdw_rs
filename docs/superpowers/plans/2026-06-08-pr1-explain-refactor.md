# PR-1: EXPLAIN Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Spec:** `docs/superpowers/specs/2026-06-08-explain-refactor-and-batched-joins-design.md` (§2)

**Goal:** Replace `src/core/explain.rs` with a pure-Rust `ExplainReport` model and thin `emit()` adapter, add two new EXPLAIN labels (`Pushdown Skipped`, `Redis Ops`), and ship unit + integration tests.

**Architecture:** Three-file module under `src/core/explain/`: `report.rs` is pure Rust (no `pg_sys`) and unit-testable; `emit.rs` is the only `unsafe` layer that calls `ExplainPropertyText/Integer`; `mod.rs` holds the two `#[pg_guard]` handler functions that build a report and call `emit()`.

**Tech Stack:** Rust, pgrx 0.18.0, PostgreSQL 14–18.

---

## File Structure

**Create:**
- `src/core/explain/mod.rs` — public handlers + module exports
- `src/core/explain/report.rs` — pure-Rust `ExplainReport`, `Prop`, builders, unit tests
- `src/core/explain/emit.rs` — `unsafe` `ExplainReport::emit()`
- `src/tests/explain_tests.rs` — `#[pg_test]` integration tests

**Delete:**
- `src/core/explain.rs`

**Modify:**
- `src/core/mod.rs` — already declares `pub mod explain;`, no change required (Rust resolves `explain/mod.rs` automatically). Verify in Task 11.
- `src/core/handlers.rs` — re-export path for `explain_foreign_scan` / `explain_foreign_modify` already goes through `core::explain::*`; no change required. Verify in Task 11.
- `src/tests/mod.rs` — add `pub mod explain_tests;`
- `README.md`, `AGENTS.md`, `CLAUDE.md` — docs sync (Task 13)

---

## Acceptance for this PR

- All unit tests in `report.rs` pass under `cargo test --features pg14 --no-default-features explain::report`.
- All `#[pg_test]` in `explain_tests.rs` pass under `cargo pgrx test pg14`.
- `make before-git-push` green.
- EXPLAIN output for a hash-table scan still includes `Redis Server`, `Redis Key`, `Table Type`, `Multi-Key Mode`, `Pushdown`, `Batch Size` (no regression in label set), plus new `Pushdown Skipped` (only when applicable) and `Redis Ops`.

---

## Task 1: Scaffold the `explain` module with a failing report unit test

**Files:**
- Create: `src/core/explain/mod.rs`
- Create: `src/core/explain/report.rs`
- Create: `src/core/explain/emit.rs` (empty stub)

- [ ] **Step 1: Write the failing test** in `src/core/explain/report.rs`

```rust
//! Pure-Rust EXPLAIN report. No pg_sys dependencies — unit-testable.

#[derive(Debug, PartialEq, Eq)]
pub enum Prop {
    Text {
        label: &'static str,
        value: String,
    },
    Int {
        label: &'static str,
        unit: Option<&'static str>,
        value: i64,
    },
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ExplainReport {
    pub props: Vec<Prop>,
}

impl ExplainReport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn text(&mut self, label: &'static str, value: impl Into<String>) {
        self.props.push(Prop::Text {
            label,
            value: value.into(),
        });
    }

    pub fn int(&mut self, label: &'static str, unit: Option<&'static str>, value: i64) {
        self.props.push(Prop::Int { label, unit, value });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_report_is_empty() {
        let r = ExplainReport::new();
        assert!(r.props.is_empty());
    }

    #[test]
    fn text_pushes_text_prop() {
        let mut r = ExplainReport::new();
        r.text("Redis Server", "127.0.0.1:6379");
        assert_eq!(
            r.props,
            vec![Prop::Text {
                label: "Redis Server",
                value: "127.0.0.1:6379".to_string()
            }]
        );
    }

    #[test]
    fn int_pushes_int_prop() {
        let mut r = ExplainReport::new();
        r.int("Batch Size", Some("rows"), 256);
        assert_eq!(
            r.props,
            vec![Prop::Int {
                label: "Batch Size",
                unit: Some("rows"),
                value: 256
            }]
        );
    }
}
```

- [ ] **Step 2: Create `src/core/explain/emit.rs`** (stub for now)

```rust
//! pg_sys adapter for ExplainReport. The only unsafe layer.

use super::report::{ExplainReport, Prop};
use pgrx::pg_sys;
use std::ffi::CString;

impl ExplainReport {
    /// Emit all props into the PostgreSQL ExplainState.
    ///
    /// # Safety
    /// `es` must be a valid pointer to an `ExplainState` for the duration of this call.
    pub unsafe fn emit(&self, es: *mut pg_sys::ExplainState) {
        for prop in &self.props {
            match prop {
                Prop::Text { label, value } => {
                    let label_c = CString::new(*label).unwrap_or_default();
                    let value_c = CString::new(value.as_str()).unwrap_or_default();
                    pg_sys::ExplainPropertyText(label_c.as_ptr(), value_c.as_ptr(), es);
                }
                Prop::Int { label, unit, value } => {
                    let label_c = CString::new(*label).unwrap_or_default();
                    let unit_c = unit.map(|u| CString::new(u).unwrap_or_default());
                    let unit_ptr = unit_c.as_ref().map_or(std::ptr::null(), |c| c.as_ptr());
                    pg_sys::ExplainPropertyInteger(label_c.as_ptr(), unit_ptr, *value, es);
                }
            }
        }
    }
}
```

- [ ] **Step 3: Create `src/core/explain/mod.rs`** with re-exports only (handlers come in Task 9)

```rust
//! EXPLAIN output for foreign scans and modifies.
//!
//! Architecture: a pure-Rust `ExplainReport` (see [`report`]) collects
//! typed `Prop`s; a thin `emit()` adapter (see [`emit`]) is the only place
//! that touches `pg_sys::ExplainState`. This split lets us unit-test the
//! report builders without a PostgreSQL backend.

pub mod emit;
pub mod report;

pub use report::{ExplainReport, Prop};
```

- [ ] **Step 4: Delete the old monolithic file**

```bash
rm /home/azureuser/redis_fdw_rs/src/core/explain.rs
```

At this point `core/mod.rs`'s `pub mod explain;` resolves to `src/core/explain/mod.rs` automatically. But the old `explain_foreign_scan` and `explain_foreign_modify` are gone, so the build will fail until Task 9. We accept this between-task breakage — we will not commit until the build is clean again.

- [ ] **Step 5: Run unit tests** (these should pass — they don't depend on pg_sys)

Run: `cd /home/azureuser/redis_fdw_rs && cargo test --lib --features pg14 --no-default-features core::explain::report -- --nocapture`
Expected: 3 tests pass (`new_report_is_empty`, `text_pushes_text_prop`, `int_pushes_int_prop`). The wider build will not compile yet because the handler wiring is still TODO; that's fine — we're verifying only the new tests.

If `cargo test --lib` won't run because the wider crate fails to compile, instead validate by running `cargo check --lib --features pg14 --no-default-features 2>&1 | grep -E "explain::report|error\[" | head` and confirm no errors are reported from `core/explain/report.rs` itself.

- [ ] **Step 6: Do NOT commit yet** — the crate doesn't build. We commit at Task 10 when wiring is restored.

---

## Task 2: `add_scan_core` — server, key, type, multi-key, batch_size

**Files:**
- Modify: `src/core/explain/report.rs`

- [ ] **Step 1: Add the failing test** at the bottom of `report.rs`'s `tests` module

```rust
    #[test]
    fn scan_core_includes_server_key_type_multikey_batch() {
        let mut r = ExplainReport::new();
        r.add_scan_core_fields("127.0.0.1:6379", "user:42", "hash", false, 5000);

        assert!(matches!(
            r.props.first(),
            Some(Prop::Text { label: "Redis Server", value }) if value == "127.0.0.1:6379"
        ));
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Redis Key", value } if value == "user:42"
        )));
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Table Type", value } if value == "hash"
        )));
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Multi-Key Mode", value } if value == "false"
        )));
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Int { label: "Batch Size", unit: Some("rows"), value: 5000 }
        )));
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /home/azureuser/redis_fdw_rs && cargo test --lib --features pg14 --no-default-features core::explain::report::tests::scan_core_includes`
Expected: compile error — `add_scan_core_fields` does not exist.

- [ ] **Step 3: Implement** — add to `impl ExplainReport`

```rust
    /// Add the labels common to every scan EXPLAIN output: server, key, type,
    /// multi-key mode, batch size. Accepts primitive args (not `&RedisFdwState`)
    /// so it stays trivially unit-testable.
    pub fn add_scan_core_fields(
        &mut self,
        host_port: &str,
        key_prefix: &str,
        type_name: &str,
        is_multi_key: bool,
        batch_size: usize,
    ) {
        self.text("Redis Server", host_port);
        self.text("Redis Key", key_prefix);
        self.text("Table Type", type_name);
        self.text("Multi-Key Mode", if is_multi_key { "true" } else { "false" });
        self.int("Batch Size", Some("rows"), batch_size as i64);
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: same command as Step 2.
Expected: 1 test passes.

- [ ] **Step 5: Do NOT commit yet.**

---

## Task 3: `add_pushdown` — render pushable conditions or "none"

**Files:**
- Modify: `src/core/explain/report.rs`

- [ ] **Step 1: Add the failing tests**

```rust
    #[test]
    fn add_pushdown_with_no_analysis_emits_none() {
        let mut r = ExplainReport::new();
        r.add_pushdown_summary(None);
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Pushdown", value } if value == "none"
        )));
    }

    #[test]
    fn add_pushdown_with_conditions_lists_them() {
        use crate::query::pushdown_types::{ComparisonOperator, PushableCondition, PushdownAnalysis};
        let analysis = PushdownAnalysis {
            can_optimize: true,
            pushable_conditions: vec![
                PushableCondition {
                    column_name: "field".to_string(),
                    column_index: 0,
                    operator: ComparisonOperator::Equal,
                    value: "x".to_string(),
                },
                PushableCondition {
                    column_name: "score".to_string(),
                    column_index: 1,
                    operator: ComparisonOperator::GreaterThanOrEqual,
                    value: "10".to_string(),
                },
            ],
            limit_offset: Default::default(),
        };

        let mut r = ExplainReport::new();
        r.add_pushdown_summary(Some(&analysis));

        let pushdown = r.props.iter().find_map(|p| match p {
            Prop::Text { label: "Pushdown", value } => Some(value.clone()),
            _ => None,
        });
        let pushdown = pushdown.expect("Pushdown property missing");
        assert!(pushdown.contains("field"));
        assert!(pushdown.contains("score"));
        assert!(pushdown.contains("'x'"));
        assert!(pushdown.contains("'10'"));
    }
```

> If `PushableCondition` or `PushdownAnalysis` field names differ from above, adjust the test fixture to match `src/query/pushdown_types.rs`. The names used here match the spec; the engineer should re-read `pushdown_types.rs` and reconcile if drift exists.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib --features pg14 --no-default-features core::explain::report::tests::add_pushdown`
Expected: `add_pushdown_summary` not defined.

- [ ] **Step 3: Implement** — add to `impl ExplainReport`

```rust
    /// Render the pushdown summary. Emits `Pushdown: none` when no conditions
    /// were pushed, else a comma-separated list of `column op 'value'` clauses.
    pub fn add_pushdown_summary(
        &mut self,
        analysis: Option<&crate::query::pushdown_types::PushdownAnalysis>,
    ) {
        let desc = match analysis {
            Some(a) if a.has_optimizations() => a
                .pushable_conditions
                .iter()
                .map(|c| format!("{} {} '{}'", c.column_name, c.operator, c.value))
                .collect::<Vec<_>>()
                .join(", "),
            _ => "none".to_string(),
        };
        self.text("Pushdown", desc);
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: same command as Step 2.
Expected: 2 tests pass.

- [ ] **Step 5: Do NOT commit yet.**

---

## Task 4: `add_join` — join descriptor

**Files:**
- Modify: `src/core/explain/report.rs`

- [ ] **Step 1: Add the failing test**

```rust
    #[test]
    fn add_join_emits_join_label_and_server() {
        let mut r = ExplainReport::new();
        r.add_join_descriptor(
            "127.0.0.1:6379",
            "hash",
            "users",
            "set",
            "active",
        );

        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Redis Join", value } if value == "hash(users) x set(active)"
        )));
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Redis Server", value } if value == "127.0.0.1:6379"
        )));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib --features pg14 --no-default-features core::explain::report::tests::add_join`
Expected: `add_join_descriptor` not defined.

- [ ] **Step 3: Implement** — add to `impl ExplainReport`

```rust
    /// Render a join descriptor for FDW-to-FDW pushdown scans.
    pub fn add_join_descriptor(
        &mut self,
        host_port: &str,
        outer_type: &str,
        outer_key: &str,
        inner_type: &str,
        inner_key: &str,
    ) {
        let desc = format!(
            "{}({}) x {}({})",
            outer_type, outer_key, inner_type, inner_key
        );
        self.text("Redis Join", desc);
        self.text("Redis Server", host_port);
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: same command as Step 2.
Expected: 1 test passes.

- [ ] **Step 5: Do NOT commit yet.**

---

## Task 5: `add_rows_fetched` — analyze branch

**Files:**
- Modify: `src/core/explain/report.rs`

- [ ] **Step 1: Add the failing test**

```rust
    #[test]
    fn add_rows_fetched_emits_int_with_unit() {
        let mut r = ExplainReport::new();
        r.add_rows_fetched(123);
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Int { label: "Rows Fetched", unit: Some("rows"), value: 123 }
        )));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib --features pg14 --no-default-features core::explain::report::tests::add_rows_fetched`
Expected: undefined.

- [ ] **Step 3: Implement**

```rust
    pub fn add_rows_fetched(&mut self, row_count: u32) {
        self.int("Rows Fetched", Some("rows"), row_count as i64);
    }
```

- [ ] **Step 4: Run test, verify pass.**

- [ ] **Step 5: Do NOT commit yet.**

---

## Task 6: `add_pushdown_skip_reason` — new label for blocked pushdown

**Files:**
- Modify: `src/core/explain/report.rs`

- [ ] **Step 1: Add the failing test**

```rust
    #[test]
    fn add_pushdown_skip_reason_emits_text_only_when_some() {
        let mut r = ExplainReport::new();
        r.add_pushdown_skip_reason(None);
        assert!(!r.props.iter().any(|p| matches!(p, Prop::Text { label: "Pushdown Skipped", .. })));

        let mut r2 = ExplainReport::new();
        r2.add_pushdown_skip_reason(Some("baserestrictinfo present on inner rel"));
        assert!(r2.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Pushdown Skipped", value }
                if value == "baserestrictinfo present on inner rel"
        )));
    }
```

- [ ] **Step 2: Run test, verify it fails.**

Run: `cargo test --lib --features pg14 --no-default-features core::explain::report::tests::add_pushdown_skip_reason`
Expected: undefined.

- [ ] **Step 3: Implement**

```rust
    /// Surface *why* a pushdown was skipped, when applicable. `None` means
    /// either pushdown ran or there was nothing to push — nothing is emitted.
    pub fn add_pushdown_skip_reason(&mut self, reason: Option<&str>) {
        if let Some(r) = reason {
            self.text("Pushdown Skipped", r.to_string());
        }
    }
```

- [ ] **Step 4: Run test, verify pass.**

- [ ] **Step 5: Do NOT commit yet.**

---

## Task 7: `add_redis_ops` — surface which Redis commands will run

**Files:**
- Modify: `src/core/explain/report.rs`

- [ ] **Step 1: Add the failing test**

```rust
    #[test]
    fn add_redis_ops_joins_with_commas() {
        let mut r = ExplainReport::new();
        r.add_redis_ops(&["HMGET", "ZRANGEBYSCORE"]);
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Redis Ops", value }
                if value == "HMGET, ZRANGEBYSCORE"
        )));
    }

    #[test]
    fn add_redis_ops_empty_emits_none() {
        let mut r = ExplainReport::new();
        r.add_redis_ops(&[]);
        assert!(r.props.iter().any(|p| matches!(p,
            Prop::Text { label: "Redis Ops", value } if value == "none"
        )));
    }
```

- [ ] **Step 2: Run tests, verify they fail.**

- [ ] **Step 3: Implement**

```rust
    /// Render which Redis commands this scan will issue. Helps users understand
    /// the actual access pattern at a glance.
    pub fn add_redis_ops(&mut self, ops: &[&'static str]) {
        let desc = if ops.is_empty() {
            "none".to_string()
        } else {
            ops.join(", ")
        };
        self.text("Redis Ops", desc);
    }
```

- [ ] **Step 4: Run tests, verify pass.**

- [ ] **Step 5: Do NOT commit yet.**

---

## Task 8: `for_scan` and `for_modify` — top-level builders

**Files:**
- Modify: `src/core/explain/report.rs`

- [ ] **Step 1: Add the failing test for `for_modify`** (no `RedisFdwState` fixture needed)

```rust
    #[test]
    fn for_modify_emits_server_key_type_only() {
        let r = ExplainReport::from_modify_inputs("127.0.0.1:6379", "users:42", "hash");
        assert_eq!(r.props.len(), 3);
        assert!(matches!(&r.props[0], Prop::Text { label: "Redis Server", .. }));
        assert!(matches!(&r.props[1], Prop::Text { label: "Redis Key", .. }));
        assert!(matches!(&r.props[2], Prop::Text { label: "Table Type", .. }));
    }
```

- [ ] **Step 2: Run test, verify it fails.**

- [ ] **Step 3: Implement** — also add a `from_scan_inputs` helper that takes plain values, then the public `for_scan` / `for_modify` wrappers that take `&RedisFdwState`. The plain-input variants enable unit testing without constructing a full state.

```rust
    /// Build a scan report from raw fields. Kept separate from `for_scan` so
    /// unit tests don't have to construct a full `RedisFdwState`.
    pub fn from_scan_inputs(
        host_port: &str,
        key_prefix: &str,
        type_name: &'static str,
        is_multi_key: bool,
        batch_size: usize,
        analysis: Option<&crate::query::pushdown_types::PushdownAnalysis>,
        skip_reason: Option<&str>,
        ops: &[&'static str],
        analyze: bool,
        row_count: u32,
    ) -> Self {
        let mut r = Self::new();
        r.add_scan_core_fields(host_port, key_prefix, type_name, is_multi_key, batch_size);
        r.add_pushdown_summary(analysis);
        r.add_pushdown_skip_reason(skip_reason);
        r.add_redis_ops(ops);
        if analyze {
            r.add_rows_fetched(row_count);
        }
        r
    }

    /// Build a modify report from raw fields.
    pub fn from_modify_inputs(
        host_port: &str,
        key_prefix: &str,
        type_name: &'static str,
    ) -> Self {
        let mut r = Self::new();
        r.text("Redis Server", host_port);
        r.text("Redis Key", key_prefix);
        r.text("Table Type", type_name);
        r
    }

    /// Build a join report from raw fields.
    pub fn from_join_inputs(
        host_port: &str,
        outer_type: &str,
        outer_key: &str,
        inner_type: &str,
        inner_key: &str,
    ) -> Self {
        let mut r = Self::new();
        r.add_join_descriptor(host_port, outer_type, outer_key, inner_type, inner_key);
        r
    }

    /// Public scan entrypoint: extract everything needed from `state` then delegate.
    pub fn for_scan(state: &crate::core::state_manager::RedisFdwState, analyze: bool) -> Self {
        if state.is_join_scan {
            // Prefer the descriptor we have; fall back to a generic label.
            if let Some(js) = state.join_state.as_ref() {
                return Self::from_join_inputs(
                    &state.host_port,
                    js.outer_table_type.redis_type_name(),
                    &js.outer_key_prefix,
                    js.inner_table_type.redis_type_name(),
                    &js.inner_key_prefix,
                );
            }
            let mut r = Self::new();
            r.text("Redis Join", "FDW-to-FDW pushdown".to_string());
            r.text("Redis Server", state.host_port.as_str());
            return r;
        }

        let ops = redis_ops_for(state);
        let skip = pushdown_skip_reason(state);
        Self::from_scan_inputs(
            &state.host_port,
            &state.table_key_prefix,
            state.table_type.redis_type_name(),
            state.is_multi_key,
            state.batch_size,
            state.pushdown_analysis.as_ref(),
            skip,
            &ops,
            analyze,
            state.row_count,
        )
    }

    pub fn for_modify(state: &crate::core::state_manager::RedisFdwState) -> Self {
        Self::from_modify_inputs(
            &state.host_port,
            &state.table_key_prefix,
            state.table_type.redis_type_name(),
        )
    }
}

/// Compute which Redis commands this scan will issue. Static today;
/// PR-2 extends this to reflect batched/parameterized ops.
fn redis_ops_for(state: &crate::core::state_manager::RedisFdwState) -> Vec<&'static str> {
    use crate::tables::types::RedisTableType;
    match &state.table_type {
        RedisTableType::String(_) if state.is_multi_key => vec!["SCAN", "MGET"],
        RedisTableType::String(_) => vec!["GET"],
        RedisTableType::Hash(_) if state.is_multi_key => vec!["SCAN", "HGETALL"],
        RedisTableType::Hash(_) => vec!["HGETALL"],
        RedisTableType::List(_) if state.is_multi_key => vec!["SCAN", "LRANGE"],
        RedisTableType::List(_) => vec!["LRANGE"],
        RedisTableType::Set(_) if state.is_multi_key => vec!["SCAN", "SMEMBERS"],
        RedisTableType::Set(_) => vec!["SMEMBERS"],
        RedisTableType::ZSet(_) if state.is_multi_key => vec!["SCAN", "ZRANGE"],
        RedisTableType::ZSet(_) => vec!["ZRANGE"],
        RedisTableType::Stream(_) => vec!["XRANGE"],
        RedisTableType::None => vec![],
    }
}

/// Determine whether pushdown was skipped and why. Returns `None` when no
/// skip happened (the common case).
fn pushdown_skip_reason(_state: &crate::core::state_manager::RedisFdwState) -> Option<&'static str> {
    // PR-2 will populate this when WHERE-through-join paths set a skip reason.
    // For PR-1 we always return None — the label is reserved.
    None
}
```

- [ ] **Step 4: Run the modify test**

Run: `cargo test --lib --features pg14 --no-default-features core::explain::report::tests::for_modify_emits`
Expected: 1 test passes.

- [ ] **Step 5: Run all `report` unit tests to make sure nothing regressed**

Run: `cargo test --lib --features pg14 --no-default-features core::explain::report`
Expected: all tests pass (>=11 tests by now).

- [ ] **Step 6: Do NOT commit yet.**

---

## Task 9: Wire `#[pg_guard]` handlers in `mod.rs`

**Files:**
- Modify: `src/core/explain/mod.rs`

- [ ] **Step 1: Replace the contents of `src/core/explain/mod.rs`**

```rust
//! EXPLAIN output for foreign scans and modifies.
//!
//! Architecture: a pure-Rust `ExplainReport` (see [`report`]) collects
//! typed `Prop`s; a thin `emit()` adapter (see [`emit`]) is the only place
//! that touches `pg_sys::ExplainState`. The two `#[pg_guard]` handlers
//! below are intentionally tiny — they just resolve state and delegate.

pub mod emit;
pub mod report;

pub use report::{ExplainReport, Prop};

use crate::core::state_manager::RedisFdwState;
use pgrx::prelude::*;

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn explain_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
    es: *mut pg_sys::ExplainState,
) {
    log!("---> explain_foreign_scan");
    let fdw_state = (*node).fdw_state as *mut RedisFdwState;
    if fdw_state.is_null() {
        return;
    }
    let state = &*fdw_state;
    let analyze = (*es).analyze;
    ExplainReport::for_scan(state, analyze).emit(es);
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn explain_foreign_modify(
    _mtstate: *mut pg_sys::ModifyTableState,
    rinfo: *mut pg_sys::ResultRelInfo,
    _fdw_private: *mut pg_sys::List,
    _subplan_index: ::core::ffi::c_int,
    es: *mut pg_sys::ExplainState,
) {
    log!("---> explain_foreign_modify");
    let fdw_state = (*rinfo).ri_FdwState as *mut RedisFdwState;
    if fdw_state.is_null() {
        return;
    }
    let state = &*fdw_state;
    ExplainReport::for_modify(state).emit(es);
}
```

- [ ] **Step 2: Verify the crate compiles**

Run: `cd /home/azureuser/redis_fdw_rs && cargo check --lib --features pg14 --no-default-features 2>&1 | tail -30`
Expected: no errors. Warnings are acceptable.

If you see errors about an unresolved `core::explain::*` import in `handlers.rs`, open `src/core/handlers.rs` near line 8 and confirm the import is `use crate::core::explain::{explain_foreign_modify, explain_foreign_scan};`. The new module already re-exports these via `pub(crate)`.

- [ ] **Step 3: Do NOT commit yet** — still need integration tests.

---

## Task 10: First green commit — module refactor

**Files:**
- All files created/modified in Tasks 1–9.

- [ ] **Step 1: Run unit tests**

Run: `cd /home/azureuser/redis_fdw_rs && cargo test --lib --features pg14 --no-default-features core::explain`
Expected: all `report` unit tests pass.

- [ ] **Step 2: Run clippy**

Run: `cd /home/azureuser/redis_fdw_rs && cargo clippy --all-targets --features pg14 --no-default-features 2>&1 | tail -30`
Expected: no errors. Address any warnings introduced by the new module (typically: `&'static str` lifetime annotations, missing docs are OK for `pub(crate)` items).

- [ ] **Step 3: Run formatter**

Run: `cd /home/azureuser/redis_fdw_rs && cargo fmt`
Expected: no output (clean) or formatted-in-place changes.

- [ ] **Step 4: Stage and commit**

```bash
cd /home/azureuser/redis_fdw_rs
git add src/core/explain src/core/explain.rs
git status   # confirm explain.rs is deleted and explain/ is new
git commit -m "$(cat <<'EOF'
refactor(explain): extract EXPLAIN logic into pure-Rust ExplainReport

Split src/core/explain.rs into a three-file module under src/core/explain/:
report.rs holds the pure-Rust ExplainReport and Prop enum (unit-testable
without a PG backend); emit.rs is the only unsafe layer that calls
ExplainPropertyText/Integer; mod.rs hosts the two thin #[pg_guard]
handlers. New labels Pushdown Skipped and Redis Ops are emitted via
the same builder; PR-2 populates them under batched-join paths.

No behavior change for existing EXPLAIN consumers: the label set
matches the prior output verbatim (plus the two new labels above).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: Add integration test file `src/tests/explain_tests.rs`

**Files:**
- Create: `src/tests/explain_tests.rs`
- Modify: `src/tests/mod.rs` (add `pub mod explain_tests;`)

- [ ] **Step 1: Read `src/tests/mod.rs` and confirm declarations style**

Run: `cd /home/azureuser/redis_fdw_rs && head -30 src/tests/mod.rs`

- [ ] **Step 2: Add the module declaration to `src/tests/mod.rs`**

Insert (alphabetically appropriate position, after the last `pub mod ` line):

```rust
#[cfg(any(test, feature = "pg_test"))]
pub mod explain_tests;
```

Match the cfg-gating style already used by neighboring `pub mod` lines in that file — if other test modules just use `pub mod foo;` without the cfg, use the same form.

- [ ] **Step 3: Create `src/tests/explain_tests.rs`**

```rust
//! Integration tests for EXPLAIN output. Asserts label *presence* (not exact
//! line formatting) so the tests remain stable across PG 14–18.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    /// Tear-down helper used by every test to avoid cross-test FDW pollution.
    fn drop_all() {
        let _ = Spi::run("DROP FOREIGN TABLE IF EXISTS explain_test_hash;");
        let _ = Spi::run("DROP SERVER IF EXISTS explain_test_srv CASCADE;");
        let _ = Spi::run("DROP FOREIGN DATA WRAPPER IF EXISTS explain_test_wrap CASCADE;");
    }

    fn setup_hash() {
        drop_all();
        Spi::run(
            "CREATE FOREIGN DATA WRAPPER explain_test_wrap \
             HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;",
        )
        .expect("create fdw");
        Spi::run(
            "CREATE SERVER explain_test_srv FOREIGN DATA WRAPPER explain_test_wrap \
             OPTIONS (host_port '127.0.0.1:8899');",
        )
        .expect("create server");
        Spi::run(
            "CREATE FOREIGN TABLE explain_test_hash (field text, value text) \
             SERVER explain_test_srv \
             OPTIONS (database '15', table_type 'hash', table_key_prefix 'explain_test:h1');",
        )
        .expect("create table");
    }

    fn explain_text(sql: &str) -> String {
        let q = format!("EXPLAIN (FORMAT TEXT) {sql}");
        let mut out = String::new();
        let rows = Spi::connect(|client| {
            client.select(&q, None, None)?.map(|row| {
                let line: Option<&str> = row.get(1).ok().flatten();
                Ok::<_, pgrx::spi::Error>(line.unwrap_or("").to_string())
            }).collect::<Result<Vec<_>, _>>()
        })
        .expect("explain");
        for line in rows {
            out.push_str(&line);
            out.push('\n');
        }
        out
    }

    #[pg_test]
    fn explain_text_contains_server_key_type_for_hash() {
        setup_hash();
        let out = explain_text("SELECT * FROM explain_test_hash");
        assert!(out.contains("Redis Server:"), "missing Redis Server label\n{out}");
        assert!(out.contains("Redis Key:"), "missing Redis Key label\n{out}");
        assert!(out.contains("Table Type:"), "missing Table Type label\n{out}");
        assert!(out.contains("hash"), "Table Type should mention hash\n{out}");
        assert!(out.contains("Batch Size:"), "missing Batch Size label\n{out}");
        drop_all();
    }

    #[pg_test]
    fn explain_text_contains_redis_ops_for_hash_scan() {
        setup_hash();
        let out = explain_text("SELECT * FROM explain_test_hash");
        assert!(out.contains("Redis Ops:"), "missing Redis Ops label\n{out}");
        assert!(out.contains("HGETALL"), "Redis Ops should mention HGETALL\n{out}");
        drop_all();
    }

    #[pg_test]
    fn explain_text_contains_pushdown_when_filtering_on_field() {
        setup_hash();
        Spi::run("INSERT INTO explain_test_hash VALUES ('a', '1'), ('b', '2');").ok();
        let out = explain_text("SELECT * FROM explain_test_hash WHERE field = 'a'");
        assert!(out.contains("Pushdown:"), "missing Pushdown label\n{out}");
        // Either "none" (if pushdown couldn't fire) or a description containing "field" — both are valid for PR-1.
        let pushdown_line = out
            .lines()
            .find(|l| l.contains("Pushdown:"))
            .unwrap_or("");
        assert!(
            pushdown_line.contains("none") || pushdown_line.contains("field"),
            "Pushdown line should be 'none' or describe 'field': {pushdown_line}"
        );
        drop_all();
    }

    #[pg_test]
    fn explain_modify_contains_server_key_type() {
        setup_hash();
        let out = explain_text("INSERT INTO explain_test_hash VALUES ('a', '1')");
        assert!(out.contains("Redis Server:"));
        assert!(out.contains("Redis Key:"));
        assert!(out.contains("Table Type:"));
        // Modify should NOT carry Batch Size / Pushdown labels.
        assert!(!out.contains("Batch Size:"), "Modify must not emit Batch Size");
        assert!(!out.contains("Pushdown:"), "Modify must not emit Pushdown");
        drop_all();
    }
}
```

- [ ] **Step 4: Verify compilation**

Run: `cd /home/azureuser/redis_fdw_rs && cargo check --tests --features pg14 --no-default-features 2>&1 | tail -20`
Expected: no errors.

If `Spi::connect` API differs from the snippet above (pgrx 0.18 sometimes returns rows differently), inspect another `#[pg_test]` in `src/tests/` and copy that idiom — e.g. `src/tests/integration_tests.rs` is a reliable reference.

- [ ] **Step 5: Run the new tests**

First make sure Redis is up: `make setup-redis`.

Then run: `cd /home/azureuser/redis_fdw_rs && cargo pgrx test pg14 tests::explain_tests 2>&1 | tail -40`
Expected: 4 tests pass.

- [ ] **Step 6: Commit**

```bash
cd /home/azureuser/redis_fdw_rs
git add src/tests/mod.rs src/tests/explain_tests.rs
git commit -m "$(cat <<'EOF'
test(explain): add #[pg_test] coverage for new EXPLAIN labels

Asserts label presence (not exact formatting) for Redis Server, Redis Key,
Table Type, Batch Size, Redis Ops, and Pushdown on scan; and the absence
of scan-only labels on modify EXPLAIN. Stable across PG 14-18.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 12: Update README.md

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Locate the section that documents EXPLAIN**

Run: `cd /home/azureuser/redis_fdw_rs && grep -n -i "EXPLAIN\|explain output" README.md | head`

- [ ] **Step 2: Replace or extend the EXPLAIN section with the canonical label list**

Edit `README.md` — find the EXPLAIN section (if absent, create a new top-level `## EXPLAIN Output` section after the JOIN section). The content to land:

```markdown
## EXPLAIN Output

The FDW emits structured properties via PostgreSQL's `ExplainPropertyText`/`Integer`
API. `EXPLAIN (FORMAT TEXT)` includes:

| Label | When | Meaning |
|---|---|---|
| `Redis Server` | scan + modify + join | `host:port` of the Redis endpoint |
| `Redis Key` | scan + modify | The `table_key_prefix` |
| `Table Type` | scan + modify | `hash`, `string`, `list`, `set`, `zset`, `stream` |
| `Multi-Key Mode` | scan | `true` if `table_key_prefix` contains a glob |
| `Pushdown` | scan | Comma-separated `col op 'val'`, or `none` |
| `Pushdown Skipped` | scan (when blocked) | Reason pushdown could not fire (e.g. base restriction blocked join pushdown) |
| `Redis Ops` | scan | Redis commands the scan will issue (e.g. `HGETALL`, `MGET`) |
| `Batch Size` | scan | `batch_size` table option, in rows |
| `Redis Join` | join | `outer_type(prefix) x inner_type(prefix)` |
| `Rows Fetched` | scan + `ANALYZE` | Actual row count after execution |

Example:

\`\`\`sql
EXPLAIN (FORMAT TEXT) SELECT * FROM redis_users WHERE field = 'alice';
\`\`\`

\`\`\`
Foreign Scan on redis_users
  Redis Server: 127.0.0.1:6379
  Redis Key: users:profile
  Table Type: hash
  Multi-Key Mode: false
  Pushdown: field = 'alice'
  Redis Ops: HGETALL
  Batch Size: 5000 rows
\`\`\`
```

(Note: replace the escaped backticks with real triple backticks when writing the markdown.)

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(readme): document the EXPLAIN label set"
```

---

## Task 13: Sync AGENTS.md and CLAUDE.md

**Files:**
- Modify: `AGENTS.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Find the "Module Structure" section in both files**

Run: `grep -n "src/core/" AGENTS.md CLAUDE.md | head`

- [ ] **Step 2: Update the `src/core/` description line**

In both `AGENTS.md` and `CLAUDE.md`, replace:

```
- `src/core/` — FDW handler callbacks (`handlers.rs`), join pushdown logic (`join_handlers.rs`), EXPLAIN output (`explain.rs`), schema import & analyze ...
```

with:

```
- `src/core/` — FDW handler callbacks (`handlers.rs`), join pushdown logic (`join_handlers.rs`), EXPLAIN output (`explain/` — `report.rs` pure-Rust model, `emit.rs` pg_sys adapter, `mod.rs` handlers), schema import & analyze ...
```

Use the `Edit` tool with the full surrounding sentence as `old_string` to make the replacement unique.

- [ ] **Step 3: Add an EXPLAIN labels reference**

In both files, locate the "FDW Lifecycle" section's Explain bullet (point 3 of the lifecycle). Replace:

```
3. **Explain**: `explain_foreign_scan`, `explain_foreign_modify` (EXPLAIN output with server, key, type, pushdown, batch info)
```

with:

```
3. **Explain**: `explain_foreign_scan`, `explain_foreign_modify`. Output is built by `ExplainReport` (pure Rust in `src/core/explain/report.rs`) then rendered via `emit()`. Labels: `Redis Server`, `Redis Key`, `Table Type`, `Multi-Key Mode`, `Pushdown`, `Pushdown Skipped` (when blocked), `Redis Ops`, `Batch Size`. Join scans emit `Redis Join` and `Redis Server`. `ANALYZE` adds `Rows Fetched`.
```

- [ ] **Step 4: Commit**

```bash
git add AGENTS.md CLAUDE.md
git commit -m "docs(agents,claude): document explain/ module split and label set"
```

---

## Task 14: Final acceptance — `make before-git-push`

**Files:** None.

- [ ] **Step 1: Ensure Redis is up**

Run: `cd /home/azureuser/redis_fdw_rs && make setup-redis`
Expected: standalone Redis listening on `127.0.0.1:8899`.

- [ ] **Step 2: Run the gate**

Run: `cd /home/azureuser/redis_fdw_rs && make before-git-push 2>&1 | tail -60`
Expected: exit status 0. The output should include passing counts for the new `tests::explain_tests` module.

- [ ] **Step 3: If failures occur**, address them; do not skip with `--no-verify`. Re-run until clean.

- [ ] **Step 4: No new commit needed** — `before-git-push` should not produce file changes. If it did (e.g. `cargo fmt` reformatted), commit those changes:

```bash
git status
git diff
git add -u
git commit -m "chore: apply cargo fmt"
```

- [ ] **Step 5: Push when user is ready** (do NOT push without user approval — gate via the user, not automation).

---

## Self-Review Checklist (completed during plan authoring)

- ✅ Every task lists exact file paths.
- ✅ Every code step shows the full code; no "similar to Task N" forwards.
- ✅ Every test is shown in full before its implementation.
- ✅ Every command is exact, including working directory.
- ✅ Spec §2 (EXPLAIN refactor) coverage: report+emit split → Tasks 1–9; new labels → Tasks 6–7; unit tests → Tasks 1–8; integration tests → Task 11; docs sync → Tasks 12–13.
- ✅ No placeholders, no TBDs, no "add validation".
- ✅ Type names consistent: `ExplainReport`, `Prop`, `PushdownAnalysis`, `RedisFdwState` used identically across all tasks.
