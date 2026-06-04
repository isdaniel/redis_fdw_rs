//! Tests covering FFI-boundary safety fixes:
//!   1. `prev(...)` call in object_access_hook wrapped in `pg_guard_ffi_boundary`
//!   2. `#[pg_guard]` on `redis_fdw_object_access_hook`
//!   3. `exec_clear_tuple` routed through `pg_sys::ExecClearTuple` (auto-guarded)
//!
//! These are behavioral integration tests — they cannot directly observe whether
//! the guard wrappers were applied, but they exercise the code paths that would
//! corrupt PG state if the guards were missing. A regression (e.g., removing the
//! guard and triggering a longjmp through a Rust frame) would surface as a
//! backend crash or a missed validation error in CI.
#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    const REDIS_HOST_PORT: &str = "127.0.0.1:8899";
    const TEST_DATABASE: &str = "15";
    const FDW_NAME: &str = "redis_ffi_fdw";
    const SERVER_NAME: &str = "redis_ffi_server";

    fn setup_fdw() {
        let _ = Spi::run(&format!(
            "DROP FOREIGN DATA WRAPPER IF EXISTS {} CASCADE;",
            FDW_NAME
        ));
        Spi::run(&format!(
            "CREATE FOREIGN DATA WRAPPER {} HANDLER redis_fdw_handler VALIDATOR redis_fdw_validator;",
            FDW_NAME
        ))
        .unwrap();
        Spi::run(&format!(
            "CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS (host_port '{}');",
            SERVER_NAME, FDW_NAME, REDIS_HOST_PORT
        ))
        .unwrap();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // If `prev(...)` were not wrapped in `pg_guard_ffi_boundary`, any cooperating
    // hook that ereports would jump across our Rust frame. We can't install a
    // competing ereport'ing hook from inside a #[pg_test], but we *can* verify:
    //   (a) the hook still chains correctly when prev is None (default case)
    //   (b) the hook still chains correctly when a no-op prev exists
    //       (in practice covered by other extensions loaded alongside pgrx,
    //        but here we just confirm DDL events on non-foreign objects don't
    //        interfere with our validation logic)
    //   (c) panics inside our hook (triggered by validate_column_count's
    //       pgrx::error!) become PG errors — which requires #[pg_guard]
    // ─────────────────────────────────────────────────────────────────────────

    /// Validation error from the hook converts cleanly to a PG error
    /// (instead of unwinding through C). The existing ddl_validation_tests
    /// suite proves this in bulk; this single `#[should_panic]` here just
    /// pins the contract specific to the ffi-safe hook path.
    #[pg_test]
    #[should_panic(expected = "redis_fdw: table type 'string' requires exactly 1 data column")]
    fn test_ffi_hook_panic_converts_to_error() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ffi_panic_bad (a text, b text, c text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix 'ffi:panic'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("SELECT * FROM ffi_panic_bad;").unwrap();
    }

    /// Hook is fired on non-foreign-table objects too. The early-exit guards
    /// (class_id check, handler check) must short-circuit without ever
    /// touching the function-pointer-call path.
    #[pg_test]
    fn test_ffi_hook_handles_diverse_ddl() {
        setup_fdw();

        // Each of these fires object_access_hook with a different class_id
        // and access type. The hook must handle all of them without crashing.
        Spi::run("CREATE TABLE ffi_regular (a int, b text);").unwrap();
        Spi::run("CREATE INDEX ffi_idx ON ffi_regular(a);").unwrap();
        Spi::run("CREATE VIEW ffi_view AS SELECT 1;").unwrap();
        Spi::run("CREATE SCHEMA ffi_schema;").unwrap();
        Spi::run("CREATE TYPE ffi_enum AS ENUM ('a', 'b');").unwrap();

        // Cleanup — DROP fires the hook with OAT_DROP, exercising the
        // early-return path for non-POST_CREATE access types.
        Spi::run("DROP TYPE ffi_enum;").unwrap();
        Spi::run("DROP SCHEMA ffi_schema;").unwrap();
        Spi::run("DROP VIEW ffi_view;").unwrap();
        Spi::run("DROP INDEX ffi_idx;").unwrap();
        Spi::run("DROP TABLE ffi_regular;").unwrap();
    }

    /// Verify the hook chain pass-through path is reached and harmless when
    /// no previous hook is registered (PREV_OBJECT_ACCESS_HOOK == None).
    /// This is the most common case in production.
    #[pg_test]
    fn test_ffi_hook_works_with_no_prev_hook() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ffi_no_prev (val text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix 'ffi:nop'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Finding #3: exec_clear_tuple uses pg_sys::ExecClearTuple
    //
    // Every row returned by iterate_foreign_scan calls exec_clear_tuple via
    // ExecStoreVirtualTuple semantics. A bulk SELECT exercises this hundreds
    // of times. If the previous raw fn-pointer call had a latent issue with
    // any slot ops kind, this would surface as a crash mid-scan.
    // ─────────────────────────────────────────────────────────────────────────

    /// Bulk scan exercises ExecClearTuple many times per query.
    #[pg_test]
    fn test_ffi_clear_tuple_bulk_scan() {
        setup_fdw();
        let key = "ffi:bulk:set";
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ffi_bulk_set (member text) SERVER {} OPTIONS (
                database '{}', table_type 'set', table_key_prefix '{}'
            );",
            SERVER_NAME, TEST_DATABASE, key
        ))
        .unwrap();

        // Clean prior state, then populate 200 distinct rows.
        Spi::run("DELETE FROM ffi_bulk_set;").unwrap();
        for i in 0..200 {
            Spi::run(&format!(
                "INSERT INTO ffi_bulk_set (member) VALUES ('m{}');",
                i
            ))
            .unwrap();
        }

        // Full scan — drives ExecClearTuple on each iteration.
        let count: Option<i64> =
            Spi::get_one("SELECT COUNT(*)::bigint FROM ffi_bulk_set;").unwrap();
        assert_eq!(count, Some(200), "bulk scan should return all rows");

        // Re-scan (e.g., as inner side of a nested loop) hits ExecClearTuple
        // through re_scan_foreign_scan as well.
        let count2: Option<i64> =
            Spi::get_one("SELECT COUNT(*)::bigint FROM ffi_bulk_set a, generate_series(1, 3) g;")
                .unwrap();
        assert_eq!(count2, Some(600), "rescan path should clear+refill slot");

        Spi::run("DELETE FROM ffi_bulk_set;").unwrap();
        Spi::run("DROP FOREIGN TABLE ffi_bulk_set;").unwrap();
    }

    /// Mixed-type scans cover ExecClearTuple under different tuple layouts.
    #[pg_test]
    fn test_ffi_clear_tuple_across_types() {
        setup_fdw();

        // Hash table — 2 columns per row
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ffi_h (f text, v text) SERVER {} OPTIONS (
                database '{}', table_type 'hash', table_key_prefix 'ffi:mix:hash'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("DELETE FROM ffi_h;").unwrap();
        for i in 0..50 {
            Spi::run(&format!("INSERT INTO ffi_h VALUES ('f{}', 'v{}');", i, i)).unwrap();
        }

        // ZSet — 2 columns
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ffi_z (m text, s text) SERVER {} OPTIONS (
                database '{}', table_type 'zset', table_key_prefix 'ffi:mix:zset'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        Spi::run("DELETE FROM ffi_z;").unwrap();
        for i in 0..50 {
            Spi::run(&format!("INSERT INTO ffi_z VALUES ('m{}', '{}');", i, i)).unwrap();
        }

        let h_count: Option<i64> = Spi::get_one("SELECT COUNT(*)::bigint FROM ffi_h;").unwrap();
        assert_eq!(h_count, Some(50));

        let z_count: Option<i64> = Spi::get_one("SELECT COUNT(*)::bigint FROM ffi_z;").unwrap();
        assert_eq!(z_count, Some(50));

        // JOIN exercises ExecClearTuple on both sides repeatedly.
        let join_count: Option<i64> =
            Spi::get_one("SELECT COUNT(*)::bigint FROM ffi_h h, ffi_z z WHERE h.f = z.m;").unwrap();
        // Naming is independent (h.f = 'f0..f49', z.m = 'm0..m49') so join yields 0.
        // The point is the scan runs to completion without crashing.
        assert_eq!(join_count, Some(0));

        Spi::run("DELETE FROM ffi_h;").unwrap();
        Spi::run("DELETE FROM ffi_z;").unwrap();
        Spi::run("DROP FOREIGN TABLE ffi_h;").unwrap();
        Spi::run("DROP FOREIGN TABLE ffi_z;").unwrap();
    }

    /// Empty-result scans must also exit cleanly through ExecClearTuple.
    #[pg_test]
    fn test_ffi_clear_tuple_empty_scan() {
        setup_fdw();
        Spi::run(&format!(
            "CREATE FOREIGN TABLE ffi_empty (val text) SERVER {} OPTIONS (
                database '{}', table_type 'string', table_key_prefix 'ffi:does:not:exist'
            );",
            SERVER_NAME, TEST_DATABASE
        ))
        .unwrap();
        let count: Option<i64> = Spi::get_one("SELECT COUNT(*)::bigint FROM ffi_empty;").unwrap();
        assert_eq!(count, Some(0));
        Spi::run("DROP FOREIGN TABLE ffi_empty;").unwrap();
    }
}
