/// Query processing and optimization module
///
/// This module handles query planning, WHERE clause pushdown optimization,
/// and other query processing enhancements for the Redis FDW.
pub mod pushdown;
pub mod pushdown_types;
pub mod scan_ops;

// Re-export for convenience
pub use pushdown::*;
pub use pushdown_types::*;
pub use scan_ops::*;
