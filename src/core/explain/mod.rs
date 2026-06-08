//! EXPLAIN output for foreign scans and modifies.
//!
//! Architecture: a pure-Rust `ExplainReport` (see [`report`]) collects
//! typed `Prop`s; a thin `emit()` adapter (see [`emit`]) is the only place
//! that touches `pg_sys::ExplainState`. The two `#[pg_guard]` handlers
//! below are intentionally tiny — they just resolve state and delegate.

pub mod emit;
pub mod report;

pub use report::ExplainReport;

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
