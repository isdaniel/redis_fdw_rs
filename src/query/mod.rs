/// Query processing and optimization module
///
/// This module handles query planning, WHERE clause pushdown optimization,
/// cost estimation, and other query processing enhancements for the Redis FDW.
pub mod cost_estimation;
pub mod limit;
pub mod pushdown;
pub mod pushdown_types;
pub mod scan_ops;
