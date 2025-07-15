# Redis FDW Testing Guide

This document describes the test suite for the Redis Foreign Data Wrapper (FDW) extension.

## Test Structure

The test suite is organized into several categories:

### 1. Unit Tests (`src/redis_fdw/tests.rs`)

These tests validate the core FDW functionality without requiring an actual Redis connection:

- **Handler Creation Tests**: Verify the FDW handler function works correctly
- **DDL Tests**: Test foreign data wrapper, server, and table creation
- **State Management Tests**: Test the internal state management logic
- **Configuration Tests**: Validate option parsing and validation
- **Schema Tests**: Test table and column definitions

### 2. Utility Tests (`src/utils_share/tests.rs`)

These tests validate the shared utility functions:

- **Data Conversion Tests**: Test cell and row data handling
- **String Handling Tests**: Test string conversion utilities
- **Memory Management Tests**: Validate memory context operations
- **Type Conversion Tests**: Test PostgreSQL data type conversions

### 3. Integration Tests (Conditional)

These tests require a running Redis server and are enabled with the `integration_tests` feature:

- **Connection Tests**: Verify actual Redis connectivity
- **Data Operation Tests**: Test real INSERT and SELECT operations
- **Error Handling Tests**: Test behavior with Redis connection issues

## Running Tests

### Basic Unit Tests

Run the standard test suite (no Redis required):

```bash
cargo pgrx test
```

### Integration Tests

To run integration tests, you need:

1. A running Redis server on `127.0.0.1:6379`
2. Enable the integration tests feature

```bash
# Start Redis server
docker run -d --name redis-test -p 6379:6379 redis

# Run tests with integration tests enabled
cargo pgrx test --features integration_tests

# Cleanup
docker stop redis-test && docker rm redis-test
```

### Specific PostgreSQL Version

Run tests against a specific PostgreSQL version:

```bash
cargo pgrx test pg14
cargo pgrx test pg15
cargo pgrx test pg16
cargo pgrx test pg17
```

## Test Coverage

### ✅ Currently Tested

- FDW handler registration and creation
- Foreign data wrapper DDL operations
- Server and table creation with options
- Hash and List table type definitions
- State management and configuration parsing
- Data type conversions and string handling
- UPDATE/DELETE operation safety (they don't crash)

### 🚧 Areas for Future Testing

- Actual Redis data operations (requires Redis)
- Error handling with invalid Redis connections
- Performance testing with large datasets
- Concurrent access patterns
- Memory leak detection
- Security and authentication testing

## Test Data Isolation

Integration tests use Redis database 15 and key prefixes like `fdw_test:*` to avoid conflicts with other data. Tests clean up after themselves, but if tests are interrupted, you may need to manually clean Redis:

```bash
# Connect to Redis and clean test data
redis-cli
> SELECT 15
> FLUSHDB
> exit
```

## Writing New Tests

### Unit Test Example

```rust
#[pg_test]
fn test_new_feature() {
    // Setup
    Spi::run("CREATE FOREIGN DATA WRAPPER test_wrapper HANDLER redis_fdw_handler;").unwrap();
    
    // Test
    let result = Spi::get_one::<String>("SELECT 'test'");
    assert_eq!(result.unwrap(), Some("test".to_string()));
    
    // Cleanup
    Spi::run("DROP FOREIGN DATA WRAPPER test_wrapper CASCADE;").unwrap();
}
```

### Integration Test Example

```rust
#[pg_test]
#[cfg(feature = "integration_tests")]
fn test_redis_integration() {
    // This test will only run when Redis is available
    // and integration_tests feature is enabled
}
```

## Test Configuration

Tests are configured in `Cargo.toml`:

```toml
[features]
pg_test = []                    # Standard pgrx test framework
integration_tests = []          # Enable Redis integration tests
```

## Continuous Integration

For CI environments, run tests in this order:

1. **Unit Tests**: `cargo pgrx test` (no external dependencies)
2. **Integration Tests**: Start Redis, then `cargo pgrx test --features integration_tests`
3. **Multi-version Tests**: Repeat for each supported PostgreSQL version

## Test Debugging

Enable verbose logging during tests:

```bash
# Set PostgreSQL log level
export PGRS_LOG_LEVEL=debug

# Run tests with verbose output
cargo pgrx test -- --nocapture
```

View test output in PostgreSQL logs:

```bash
# Check pgrx test logs
tail -f ~/.pgrx/data-*/postgresql.conf
```

## Known Test Limitations

1. **Redis Dependency**: Integration tests require Redis server
2. **Timing Issues**: Some tests may be sensitive to timing with external Redis
3. **Database State**: Tests assume clean PostgreSQL database state
4. **Platform Differences**: Some tests may behave differently on different operating systems

## Contributing Test Cases

When adding new features, please include:

1. **Unit tests** for core functionality
2. **Integration tests** for Redis operations
3. **Error case tests** for failure scenarios
4. **Documentation** in this file for complex test scenarios

Follow the existing test patterns and ensure all tests clean up after themselves.
