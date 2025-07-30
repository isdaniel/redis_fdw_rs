/// Comprehensive test suite for Redis FDW
///
/// This module organizes all tests by functionality area, making it easier
/// to locate and maintain tests for specific components.

#[cfg(any(test, feature = "pg_test"))]
pub mod core_tests;

#[cfg(any(test, feature = "pg_test"))]
pub mod table_tests;

#[cfg(any(test, feature = "pg_test"))]
pub mod pushdown_tests;

#[cfg(any(test, feature = "pg_test"))]
pub mod utils_tests;

#[cfg(any(test, feature = "pg_test"))]
pub mod basic_test;

#[cfg(any(test, feature = "pg_test"))]
pub mod integration_tests;

#[cfg(any(test, feature = "pg_test"))]
pub mod cluster_integration_tests;

#[cfg(any(test, feature = "pg_test"))]
pub mod auth_tests;

