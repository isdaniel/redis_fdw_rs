# Redis FDW Testing Guide

This document describes the test suite for the Redis Foreign Data Wrapper (FDW) extension.

## Test Structure

The test suite is organized into several categories in the `src/tests/` directory:

### 1. Core Tests (`src/tests/core_tests.rs`)

These tests validate the core FDW functionality and Redis connections:

- **Handler Creation Tests**: Verify the FDW handler function works correctly
- **DDL Tests**: Test foreign data wrapper, server, and table creation
- **State Management Tests**: Test the internal state management logic
- **Configuration Tests**: Validate option parsing and validation
- **Connection Tests**: Test Redis connection creation and management

### 2. Table Implementation Tests (`src/tests/table_tests.rs`)

These tests validate the table-specific implementations:

- **Hash Table Tests**: Test Redis hash table operations
- **List Table Tests**: Test Redis list table operations  
- **Set Table Tests**: Test Redis set table operations
- **String Table Tests**: Test Redis string table operations
- **ZSet Table Tests**: Test Redis sorted set table operations
- **Unified Interface Tests**: Test the common RedisTableOperations trait

### 3. Integration Tests (`src/tests/integration_tests.rs`)

These tests require a running Redis server and test actual Redis operations:

- **CRUD Operations**: Test real INSERT, SELECT, and DELETE operations
- **Data Type Tests**: Test all supported Redis data types
- **Bulk Operations**: Test batch insert and delete operations
- **Error Handling Tests**: Test behavior with Redis connection issues

### 4. Stream Tests (`src/tests/stream_test.rs`)

Dedicated tests for Redis Stream functionality:

- **Stream Operations**: Test stream insert, select, and delete
- **Large Data Handling**: Test pagination and batch processing
- **Range Queries**: Test time-based stream queries
- **Integration**: Test actual Redis stream operations

### 5. Cluster Tests (`src/tests/cluster_integration_tests.rs`)

Tests for Redis cluster functionality:

- **Cluster Connectivity**: Test connection to Redis clusters
- **Key Distribution**: Test data distribution across cluster nodes
- **Failover Handling**: Test cluster resilience

### 6. Query Pushdown Tests (`src/tests/pushdown_tests.rs`)

Tests for WHERE clause optimization:

- **Pushdown Logic**: Test condition pushdown to Redis
- **Performance Tests**: Test query optimization effectiveness
- **Mixed Conditions**: Test combinations of pushable and non-pushable conditions

### 7. Utility Tests (`src/tests/utils_tests.rs`)

These tests validate the shared utility functions:

- **Data Conversion Tests**: Test cell and row data handling
- **String Handling Tests**: Test string conversion utilities
- **Memory Management Tests**: Validate memory context operations
- **Type Conversion Tests**: Test PostgreSQL data type conversions

### 8. Authentication Tests (`src/tests/auth_tests.rs`)

Tests for Redis authentication and connection configuration:

- **Authentication Tests**: Test Redis password authentication
- **Configuration Tests**: Test connection string parsing
- **Cluster Authentication**: Test authentication with Redis clusters

## Running Tests

### Basic Unit Tests

Run the standard test suite (no Redis required):

```bash
cargo pgrx test
```

### Integration Tests

To run integration tests, you need a running Redis server:

```bash
# Start Redis server
docker run -d --name redis-test -p 6379:6379 redis

# Run tests (integration tests are included by default)
cargo pgrx test

# Cleanup
docker stop redis-test && docker rm redis-test
```

**Note**: Unlike mentioned in earlier documentation, there is no separate `integration_tests` feature flag. Integration tests are included in the main test suite and will attempt to connect to Redis if available.

### Specific PostgreSQL Version

Run tests against a specific PostgreSQL version:

```bash
cargo pgrx test pg14
cargo pgrx test pg15
cargo pgrx test pg16
cargo pgrx test pg17
```

## Test Coverage

## Test Coverage

### ✅ Currently Tested

- FDW handler registration and creation
- Foreign data wrapper DDL operations
- Server and table creation with options
- All Redis table type definitions (Hash, List, Set, ZSet, String, Stream)
- State management and configuration parsing
- Data type conversions and string handling
- All CRUD operations (SELECT, INSERT, DELETE) for all table types
- UPDATE operation safety (they correctly fail as unsupported)
- Redis Cluster connectivity and operations
- WHERE clause pushdown optimization for supported conditions
- Redis Stream operations with large data set handling
- Authentication and connection management
- Connection pooling with R2D2
- Query performance optimization

### 🚧 Areas for Future Testing

- Performance testing with very large datasets
- Advanced concurrent access patterns
- Memory leak detection under high load
- Security edge cases and advanced authentication scenarios
- Multi-database concurrent operations
- Network failure recovery testing

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

### Standard Test Example

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
fn test_redis_integration() {
    // This test will run as part of the main test suite
    // and will attempt to connect to Redis if available
    // No special feature flag required
}
```

## Test Configuration

Tests are configured in `Cargo.toml`:

```toml
[features]
pg_test = []                    # Standard pgrx test framework
```

The project has the following test-related features for different PostgreSQL versions:
- `pg14`, `pg15`, `pg16`, `pg17` - PostgreSQL version-specific features

## Continuous Integration

For CI environments, run tests in this order:

1. **Unit Tests**: `cargo test` (Rust unit tests, no external dependencies)
2. **PostgreSQL Tests**: `cargo pgrx test` (includes integration tests if Redis is available)
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
# Check pgrx test logs (adjust path for your PostgreSQL version)
tail -f ~/.pgrx/data-*/postgresql.conf
```

## Known Test Limitations

1. **Redis Dependency**: Some integration tests require a Redis server for full functionality
2. **Timing Issues**: Some tests may be sensitive to timing with external Redis
3. **Database State**: Tests assume clean PostgreSQL database state
4. **Platform Differences**: Some tests may behave differently on different operating systems
5. **Connection Pool**: Connection pool tests require proper Redis server connectivity

## Contributing Test Cases

When adding new features, please include:

1. **Unit tests** for core functionality  
2. **Integration tests** for Redis operations
3. **Error case tests** for failure scenarios
4. **Documentation** in this file for complex test scenarios

Follow the existing test patterns and ensure all tests clean up after themselves. Tests are organized by functionality in the `src/tests/` directory with clear separation between core, table, integration, and utility tests.
