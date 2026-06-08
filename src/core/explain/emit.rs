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
