mod async_runtime;
mod redis_fdw;
mod utils_share;

::pgrx::pg_module_magic!(name, version);

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // Initialize the global Tokio runtime when tests start
        let _ = crate::async_runtime::get_runtime();
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
