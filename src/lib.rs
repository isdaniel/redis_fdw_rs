use pgrx::prelude::*;

// Core FDW functionality
mod core;

// Query processing and optimization
mod query;

// Table type implementations
mod tables;

// Utility functions and helpers
mod utils;

// All tests organized by functionality
#[cfg(any(test, feature = "pg_test"))]
mod tests;

// Re-export the main FDW handler function for PostgreSQL
pub use core::handlers::redis_fdw_handler;

::pgrx::pg_module_magic!(name, version);

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
