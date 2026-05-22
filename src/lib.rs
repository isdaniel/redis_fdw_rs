// Authentication module for Redis credentials
mod auth;

// Core FDW functionality
mod core;

// Query processing and optimization
mod query;

// JOIN support (parameterized paths and FDW-to-FDW pushdown)
mod join;

// Table type implementations
mod tables;

// Utility functions and helpers
mod utils;

// All tests organized by functionality
#[cfg(any(test, feature = "pg_test"))]
mod tests;

// Re-export the main FDW handler function for PostgreSQL
pub use core::handlers::redis_fdw_handler;
pub use core::validator::redis_fdw_validator_wrapper;

::pgrx::pg_module_magic!(name, version);

#[allow(non_snake_case)]
#[pgrx::pg_guard]
pub unsafe extern "C-unwind" fn _PG_init() {
    core::ddl_hook::init_hook();
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries = 'redis_fdw_rs'"]
    }
}
