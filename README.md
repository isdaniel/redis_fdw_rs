# Redis Foreign Data Wrapper for PostgreSQL (Rust)

A high-performance Redis Foreign Data Wrapper (FDW) for PostgreSQL written in Rust using the [pgrx](https://github.com/pgcentralfoundation/pgrx) framework. This extension allows PostgreSQL to directly query and manipulate Redis data as if it were regular PostgreSQL tables, with full support for Redis Streams and large data set handling.

## Features

- **High-performance data access** from Redis to PostgreSQL
- **Redis Cluster support** with automatic failover and sharding across multiple nodes
- **Connection pooling** with R2D2 for efficient connection reuse and resource management
- **WHERE clause pushdown optimization** for significantly improved query performance
- **Redis data types support**: Hash, List, Set, ZSet, String, and Stream (with varying levels of implementation)
- **Redis Streams support**: Full implementation with large data set handling, pagination, and time-based queries
- **Large data set optimization**: Configurable batch processing and streaming access for Redis Streams
- **Supported operations**: SELECT, INSERT, DELETE (UPDATE operations are not supported due to Redis data model differences)
- **Connection management** and memory optimization
- **Built with Rust** for memory safety and performance
- **Unified trait interface** providing consistent behavior across all Redis table types
- **Enhanced error handling** for robust foreign data operations
- **Compatible with PostgreSQL 14-17**

## Prerequisites

- PostgreSQL 14, 15, 16, or 17
- Redis server
- Rust toolchain (for building from source)

## Installation

### Quick Start with Docker

1. **Start Redis server:**

```bash
# init single redis
docker run -d --name redis-server -p 8899:6379 redis
```

```bash
# init cluster reids
docker-compose -f docker-compose.cluster-test.yml up -d
```


2. **Build and install the extension:**

```bash
cargo pgrx install --version 0.15.0
```

3. **Create the extension in PostgreSQL:**

```bash
cargo pgrx run
```

```sql
CREATE EXTENSION redis_fdw_rs;
```

## Quick Example

Here's a complete example showing how to set up and use the Redis FDW:

```sql
-- 1. Create the foreign data wrapper
CREATE FOREIGN DATA WRAPPER redis_wrapper 
HANDLER redis_fdw_handler;

-- 2. Create a server pointing to your Redis instance
CREATE SERVER redis_server 
FOREIGN DATA WRAPPER redis_wrapper
OPTIONS (host_port '127.0.0.1:8899');

-- 3. Create a foreign table for Redis hash
CREATE FOREIGN TABLE user_profiles (key text, value text) 
SERVER redis_server
OPTIONS (
    database '0',
    table_type 'hash',
    table_key_prefix 'user:profiles'
);

-- 4. Create a foreign table for Redis list  
CREATE FOREIGN TABLE task_queue (element text) 
SERVER redis_server
OPTIONS (
    database '0',
    table_type 'list',
    table_key_prefix 'tasks:pending'
);

-- 5. Use the tables
INSERT INTO user_profiles VALUES ('name', 'John Doe');
INSERT INTO user_profiles VALUES ('email', 'john@example.com');
SELECT * FROM user_profiles;

INSERT INTO task_queue VALUES ('Process invoice #123');
INSERT INTO task_queue VALUES ('Send welcome email');
SELECT * FROM task_queue;

-- 6. Create a foreign table for Redis stream (event logging)
CREATE FOREIGN TABLE event_log (
    stream_id TEXT,
    event_type TEXT,
    event_data TEXT,
    user_id TEXT,
    timestamp_data TEXT
) 
SERVER redis_server
OPTIONS (
    database '0',
    table_type 'stream',
    table_key_prefix 'app:events'
);

-- 7. Use the stream table for event logging
INSERT INTO event_log VALUES ('*', 'user_action', 'login', '123', '2024-01-01');
INSERT INTO event_log VALUES ('*', 'user_action', 'purchase', '456', '2024-01-01');
SELECT * FROM event_log ORDER BY stream_id;
```

Example result.

```sql
redis_fdw_rs=# INSERT INTO user_profiles (key, value) 
SELECT i, 'value_' || i
FROM generate_series(1,100000) i;
INSERT 0 100000
Time: 12911.183 ms (00:12.911)
redis_fdw_rs=# SELECT * FROM user_profiles where key = '5';
 key |  value  
-----+---------
 5   | value_5
(1 row)

Time: 15.380 ms
redis_fdw_rs=# SELECT * FROM user_profiles where key in ('10', '15', '20');
 key |  value   
-----+----------
 10  | value_10
 15  | value_15
 20  | value_20
(3 rows)

redis_fdw_rs=#  SELECT * FROM user_profiles where key like '555%';
  key  |    value    
-------+-------------
 55556 | value_55556
 55581 | value_55581
 55569 | value_55569
 55561 | value_55561
 55516 | value_55516
 55538 | value_55538
 55549 | value_55549
 55539 | value_55539
 55531 | value_55531
 55545 | value_55545
 55590 | value_55590
 55512 | value_55512
 55523 | value_55523
 55534 | value_55534
 55518 | value_55518
 55560 | value_55560
 55564 | value_55564
 55592 | value_55592
 55572 | value_55572
 55519 | value_55519
 55526 | value_55526
 5559  | value_5559
 55530 | value_55530
 55511 | value_55511
 55562 | value_55562
 55542 | value_55542
 55582 | value_55582
 55580 | value_55580
 55501 | value_55501
 55540 | value_55540
 55554 | value_55554
 55546 | value_55546
 55513 | value_55513
 55548 | value_55548
--More--
```


## Configuration

### Connection Pooling

Redis FDW now includes built-in connection pooling using the R2D2 library, providing significant performance improvements and better resource management. The connection pool is automatically configured with sensible defaults but can be customized if needed.

#### Connection Pool Benefits
- **Performance**: Eliminates the overhead of establishing new Redis connections for each operation
- **Resource Efficiency**: Maintains a controlled number of connections, preventing resource exhaustion
- **Reliability**: Automatic connection health checks and recovery from network issues
- **Thread Safety**: Safe concurrent access from multiple PostgreSQL worker processes

#### Default Pool Configuration
```rust
// Default R2D2 pool settings (automatically applied)
Pool::builder()
    .max_size(10)                    // Maximum 10 connections
    .min_idle(None)                  // Minimum idle connections (defaults to max_size)
    .connection_timeout(Duration::from_secs(30))  // 30 second connection timeout
    .idle_timeout(Some(Duration::from_secs(600))) // 10 minute idle timeout
    .max_lifetime(Some(Duration::from_secs(1800))) // 30 minute max lifetime
    .test_on_check_out(true)         // Validate connections before use
    .build(redis_client)
```

#### Pool Behavior
- **Connection Reuse**: Connections are automatically returned to the pool after each operation
- **Health Monitoring**: Connections are validated before use and replaced if unhealthy
- **Automatic Cleanup**: Idle connections are closed after timeout to free resources
- **Graceful Degradation**: Pool handles connection failures transparently
- **Thread Safe**: Multiple PostgreSQL backends can safely share the connection pool

### 1. Create Foreign Data Wrapper
```sql
CREATE FOREIGN DATA WRAPPER redis_wrapper 
HANDLER redis_fdw_handler;
```

### 2. Create Server
```sql
-- Single Redis node server
CREATE SERVER redis_server 
FOREIGN DATA WRAPPER redis_wrapper
OPTIONS (
    host_port '127.0.0.1:8899',
    password 'your_redis_password'
);
```

### 3. Redis Cluster Support

Redis FDW supports both single-node and cluster deployments. To connect to a Redis cluster, specify multiple nodes in the `host_port` option using comma-separated addresses:

```sql
-- Redis Cluster server (automatic failover and sharding)
CREATE SERVER redis_cluster_server 
FOREIGN DATA WRAPPER redis_wrapper
OPTIONS (
    host_port '127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002',
    password 'your_redis_password'
);

-- You can mix IP addresses and hostnames
CREATE SERVER redis_cluster_prod 
FOREIGN DATA WRAPPER redis_wrapper
OPTIONS (
    host_port 'redis-node1.example.com:7000,redis-node2.example.com:7001,redis-node3.example.com:7002',
    password 'your_redis_password'
);
```

* Password is Optional setting.

**Cluster Benefits:**
- **Automatic failover**: If nodes fail, operations continue on healthy nodes
- **Sharding**: Data is automatically distributed across cluster nodes
- **Discovery**: Only one cluster node address is needed - the client discovers all nodes
- **High availability**: Read/write operations continue during node failures

**Cluster Usage Example:**

```sql
-- Create a foreign table using cluster connection
CREATE FOREIGN TABLE user_sessions (field TEXT, value TEXT) 
SERVER redis_cluster_server
OPTIONS (
    database '0',
    table_type 'hash',
    table_key_prefix 'session:active'
);

-- Operations work identically to single-node setup
INSERT INTO user_sessions VALUES ('user123', 'session_token_abc');
SELECT * FROM user_sessions WHERE field = 'user123';
```

## Supported Redis Data Types (Usage Examples)

### Table Type Characteristics

#### String Table (`table_type 'string'`)
- **Purpose**: Store a single string value
- **SQL Columns**: 1 column (value)
- **Use Cases**: Configuration values, counters, simple key-value storage
- **Redis Commands**: SET, GET, DEL

#### Hash Table (`table_type 'hash'`)
- **Purpose**: Store field-value pairs (like a dictionary/map)
- **SQL Columns**: 2 columns (field, value)  
- **Use Cases**: User profiles, object attributes, structured data
- **Redis Commands**: HSET, HGETALL, HDEL

#### List Table (`table_type 'list'`)
- **Purpose**: Store ordered sequence of elements
- **SQL Columns**: 1 column (element)
- **Use Cases**: Task queues, activity feeds, ordered collections
- **Redis Commands**: RPUSH, LRANGE, LREM

#### Set Table (`table_type 'set'`)
- **Purpose**: Store unordered collection of unique elements
- **SQL Columns**: 1 column (member)
- **Use Cases**: Tags, categories, unique collections
- **Redis Commands**: SADD, SMEMBERS, SREM

#### Sorted Set Table (`table_type 'zset'`)
- **Purpose**: Store ordered collection with scores
- **SQL Columns**: 2 columns (member, score)
- **Use Cases**: Leaderboards, rankings, priority queues
- **Redis Commands**: ZADD, ZRANGE, ZREM

#### Stream Table (`table_type 'stream'`)
- **Purpose**: Store append-only log of structured entries with automatic IDs
- **SQL Columns**: Multiple columns (stream_id, field1, value1, field2, value2, ...)
- **Use Cases**: Event logs, time-series data, message streams, audit trails
- **Redis Commands**: XADD, XRANGE, XDEL, XLEN
- **Features**: 
  - **Large Data Set Support**: Configurable batch processing (default: 1000 entries)
  - **Time-based Queries**: Efficient range queries using stream IDs
  - **Pagination**: Automatic streaming through large data sets
  - **Pushdown Optimization**: WHERE clause optimization for stream ID filtering

### SQL Table Definitions
```sql
-- String table (single value storage)
CREATE FOREIGN TABLE redis_string (value TEXT)
SERVER redis_server OPTIONS (table_type 'string', table_key_prefix 'config:app_name');

-- Hash table
CREATE FOREIGN TABLE redis_hash (field TEXT, value TEXT) 
SERVER redis_server OPTIONS (table_type 'hash', table_key_prefix 'user:1');

-- List table  
CREATE FOREIGN TABLE redis_list (element TEXT)
SERVER redis_server OPTIONS (table_type 'list', table_key_prefix 'items');

-- Set table
CREATE FOREIGN TABLE redis_set (member TEXT)
SERVER redis_server OPTIONS (table_type 'set', table_key_prefix 'tags');

-- Sorted set table
CREATE FOREIGN TABLE redis_zset (member TEXT, score FLOAT8)
SERVER redis_server OPTIONS (table_type 'zset', table_key_prefix 'leaderboard');

-- Stream table (for event logs and time-series data)
CREATE FOREIGN TABLE redis_stream (
    stream_id TEXT,
    field1 TEXT,
    value1 TEXT,
    field2 TEXT, 
    value2 TEXT
)
SERVER redis_server OPTIONS (table_type 'stream', table_key_prefix 'events');
```

### SQL Operations
```sql
-- String operations
INSERT INTO redis_string VALUES ('MyApplicationName');
SELECT * FROM redis_string;
-- UPDATE is not supported for Redis FDW
-- To change a value, delete and insert:
DELETE FROM redis_string WHERE value = 'MyApplicationName';
INSERT INTO redis_string VALUES ('UpdatedAppName');

-- Hash operations
INSERT INTO redis_hash VALUES ('name', 'John'), ('age', '30');
SELECT * FROM redis_hash;

-- List operations
INSERT INTO redis_list VALUES ('apple'), ('banana');
SELECT * FROM redis_list;

-- Set operations  
INSERT INTO redis_set VALUES ('red'), ('green'), ('blue');
SELECT * FROM redis_set;

-- Sorted set operations
INSERT INTO redis_zset VALUES ('player1', 100.5), ('player2', 95.0);
SELECT * FROM redis_zset ORDER BY score DESC;

-- Stream operations
INSERT INTO redis_stream VALUES ('*', 'user_id', '123', 'action', 'login');
INSERT INTO redis_stream VALUES ('*', 'user_id', '456', 'action', 'logout');
SELECT * FROM redis_stream ORDER BY stream_id;

-- Time-based queries with pushdown optimization
SELECT * FROM redis_stream 
WHERE stream_id >= '1640995200000-0' 
ORDER BY stream_id;

-- Large data set pagination (automatically handled)
SELECT * FROM redis_stream LIMIT 1000;
```

## Configuration Options

### Server Options
- `host_port`: Redis connection string (format: `host:port`) - **Required**

### Table Options
- `database`: Redis database number (default: 0) - **Optional**
- `table_type`: Redis data type - **Required**
  - `'string'` - Partial implementation ✅ (SELECT, INSERT, DELETE; UPDATE not supported)
  - `'hash'` - Partial implementation ✅ (SELECT, INSERT, DELETE; UPDATE not supported)
  - `'list'` - Partial implementation ✅ (SELECT, INSERT, DELETE; UPDATE not supported)
  - `'set'` - Partial implementation ✅ (SELECT, INSERT, DELETE; UPDATE not supported)
  - `'zset'` - Partial implementation ✅ (SELECT, INSERT, DELETE; UPDATE not supported)
  - `'stream'` - Full implementation ✅ (SELECT, INSERT, DELETE; Large data set support with pagination)
- `table_key_prefix`: Key prefix for Redis operations - **Required**

### User Mapping Options
- `password`: Redis authentication password - **Optional**

## Advanced Usage

### Redis Streams - Large Data Set Support

Redis Streams provide powerful functionality for handling large-scale event logs, time-series data, and message streams. The Redis FDW offers full Stream support with advanced features for large data sets.

#### Stream Table Features
- **Automatic Pagination**: Configurable batch sizes (default: 1000 entries)
- **Time-based Queries**: Efficient range queries using Redis stream IDs
- **Pushdown Optimization**: WHERE clause conditions executed in Redis
- **Memory Efficient**: Streaming access without loading entire data sets

#### Stream Table Definition
```sql
-- Basic stream table for event logging
CREATE FOREIGN TABLE application_events (
    stream_id TEXT,           -- Redis stream ID (auto-generated if '*')
    event_type TEXT,          -- Event classification  
    user_id TEXT,            -- User identifier
    action TEXT,             -- Action performed
    metadata TEXT            -- Additional event data
) 
SERVER redis_server 
OPTIONS (
    database '0',
    table_type 'stream',
    table_key_prefix 'app:events'
);
```

#### Stream Operations Examples
```sql
-- Insert events (use '*' for auto-generated stream IDs)
INSERT INTO application_events VALUES 
    ('*', 'user_login', '123', 'authenticate', '{"ip":"192.168.1.1"}'),
    ('*', 'page_view', '123', 'view_dashboard', '{"page":"/dashboard"}'),
    ('*', 'user_logout', '123', 'logout', '{"session_duration":"45m"}');

-- Query recent events
SELECT * FROM application_events 
ORDER BY stream_id DESC 
LIMIT 100;

-- Time-based queries (pushdown optimized)
-- Stream IDs are timestamp-based: "timestamp-sequence"
SELECT * FROM application_events 
WHERE stream_id >= '1640995200000-0'  -- Events after specific timestamp
ORDER BY stream_id;

-- Filter by event type
SELECT stream_id, user_id, action, metadata 
FROM application_events 
WHERE event_type = 'user_login'
ORDER BY stream_id DESC;

-- Large data set handling (automatic pagination)
-- PostgreSQL will automatically fetch data in batches
SELECT COUNT(*) FROM application_events;  -- Counts all events efficiently

-- Complex analytics queries
SELECT event_type, COUNT(*) as event_count
FROM application_events 
WHERE stream_id >= '1640995200000-0'
GROUP BY event_type
ORDER BY event_count DESC;
```

#### Performance Benefits for Large Data Sets
- **Streaming Access**: No memory constraints for large streams
- **Batch Processing**: Configurable batch sizes prevent memory exhaustion  
- **Range Optimization**: Leverages Redis O(log(N)) stream indexing
- **Pushdown Queries**: Reduces data transfer between Redis and PostgreSQL

#### Use Cases
- **Event Logging**: Application events, user activities, system logs
- **Time-series Data**: Metrics, sensor data, monitoring information
- **Message Streams**: Chat messages, notifications, real-time updates  
- **Audit Trails**: Security events, data changes, compliance logging

### String Table Examples
```sql
-- Configuration storage
CREATE FOREIGN TABLE app_config (value TEXT)
SERVER redis_server OPTIONS (table_type 'string', table_key_prefix 'config:database_url');

CREATE FOREIGN TABLE app_version (value TEXT)
SERVER redis_server OPTIONS (table_type 'string', table_key_prefix 'app:version');

-- Usage
INSERT INTO app_config VALUES ('postgresql://localhost:5432/mydb');
INSERT INTO app_version VALUES ('1.2.3');

SELECT 'Database URL: ' || value FROM app_config;
SELECT 'App Version: ' || value FROM app_version;

-- Update configuration (Note: Redis FDW does not support UPDATE operations)
-- To update a string value, you need to DELETE and INSERT:
DELETE FROM app_config WHERE value = 'postgresql://localhost:5432/mydb';
INSERT INTO app_config VALUES ('postgresql://newhost:5432/mydb');
```

### Complex Queries
```sql
-- Join Redis data with PostgreSQL tables
SELECT u.name, r.value as email
FROM users u
JOIN redis_hash_table r ON r.key = 'email:' || u.id;

-- Aggregate Redis list data
SELECT COUNT(*) as task_count
FROM redis_list_table;

-- Filter Redis hash data
SELECT * FROM redis_hash_table 
WHERE key LIKE 'user_%';
```

### Bulk Operations
```sql
-- Bulk insert hash data
INSERT INTO redis_hash_table VALUES 
  ('user:1', 'John Doe'),
  ('user:2', 'Jane Smith'),
  ('user:3', 'Bob Wilson');

-- Bulk insert list data
INSERT INTO redis_list_table VALUES 
  ('Task 1'),
  ('Task 2'), 
  ('Task 3');
```

## Current Implementation Status

| Redis Type | SELECT | INSERT | UPDATE | DELETE | Status |
|------------|--------|--------|--------|--------|--------|
| Hash       | ✅     | ✅     | ❌     | ✅     | **Partial** (UPDATE not supported) |
| List       | ✅     | ✅     | ❌     | ✅     | **Partial** (UPDATE not supported) |
| Set        | ✅     | ✅     | ❌     | ✅     | **Partial** (UPDATE not supported) |
| ZSet       | ✅     | ✅     | ❌     | ✅     | **Partial** (UPDATE not supported) |
| String     | ✅     | ✅     | ❌     | ✅     | **Partial** (UPDATE not supported) |
| Stream     | ✅     | ✅     | ❌     | ✅     | **Full** (Large data set support with pagination) |

## Current Limitations

- **Transactions**: Redis operations are not transactional with PostgreSQL
- **Large Data Sets**: For most table types, all data is loaded at scan initialization. Stream tables support efficient pagination for large data sets.

## Development

### Project Structure
```
src/
├── lib.rs                    # Clean entry point with organized imports
├── auth/                     # Authentication module
│   └── mod.rs               # Redis authentication handling
├── core/                     # Core FDW functionality  
│   ├── mod.rs               # Module organization and re-exports
│   ├── connection.rs        # Redis connection types and management
│   ├── connection_factory.rs # Connection creation with R2D2 pooling
│   ├── handlers.rs          # PostgreSQL FDW handlers  
│   └── state.rs            # FDW state management
├── query/                   # Query processing & optimization
│   ├── mod.rs              # Query module organization
│   ├── pushdown.rs         # WHERE clause pushdown logic
│   ├── pushdown_types.rs   # Pushdown type definitions
│   └── scan_ops.rs         # Redis scan operation builders
├── tables/                  # Table implementations
│   ├── mod.rs              # Tables module organization
│   ├── interface.rs        # RedisTableOperations trait
│   ├── types.rs           # Table type definitions (RedisTableType enum)
│   └── implementations/    # Actual Redis table implementations
│       ├── mod.rs         # Implementations organization
│       ├── hash.rs        # Redis Hash table implementation
│       ├── list.rs        # Redis List table implementation
│       ├── set.rs         # Redis Set table implementation
│       ├── string.rs      # Redis String table implementation
│       ├── stream.rs      # Redis Stream table implementation
│       └── zset.rs        # Redis ZSet table implementation
├── utils/                  # Utility functions
│   ├── mod.rs             # Utils module organization
│   ├── cell.rs            # Cell data type handling
│   ├── memory.rs          # Memory context management
│   ├── row.rs             # Row data structures
│   └── utils.rs           # General utilities
└── tests/                 # Organized test suite
    ├── mod.rs            # Test module organization
    ├── auth_tests.rs     # Authentication tests
    ├── basic_test.rs     # Basic functionality tests
    ├── core_tests.rs     # Core functionality tests
    ├── table_tests.rs    # Table implementation tests
    ├── integration_tests.rs # Redis integration tests
    ├── cluster_integration_tests.rs # Cluster integration tests
    ├── pushdown_tests.rs # Query pushdown tests
    ├── stream_test.rs    # Stream-specific tests
    └── utils_tests.rs    # Utility function tests
```

### OOP Architecture Benefits
- **Unified Interface**: All table types implement the same `RedisTableOperations` trait
- **Encapsulation**: Each table type manages its own data structures and optimization logic
- **Extensibility**: Easy to add new Redis data types by implementing the trait
- **Maintainability**: Clear separation between table-specific logic and generic FDW operations
- **Code Reuse**: Common patterns are shared through the trait interface

### Building from Source
```bash
# Install pgrx
cargo install --locked cargo-pgrx --version 0.15.0

# Initialize pgrx
cargo pgrx init

# Build and install
cargo pgrx install --release
```

### Running Tests
```bash
# Run Rust unit tests (no Redis required)
cargo test

# Run PostgreSQL integration tests (no Redis required)
cargo pgrx test

# Run integration tests with Redis (requires Redis server)
docker run -d --name redis-test -p 8899:6379 redis
cargo pgrx test --features integration_tests
docker stop redis-test && docker rm redis-test

# Test specific PostgreSQL version
cargo pgrx test pg16
```

See [TESTING.md](TESTING.md) for detailed testing documentation.

## Architecture Overview

### Object-Oriented Design with Connection Pooling

The Redis FDW follows a clean object-oriented architecture with integrated connection pooling for optimal performance:

#### Connection Management
- **R2D2 Connection Pool**: Centralized connection pool management using the industry-standard R2D2 library
- **Connection Reuse**: Automatic connection lifecycle management with pooling, validation, and cleanup
- **Thread Safety**: Safe concurrent access from multiple PostgreSQL worker processes
- **Health Monitoring**: Automatic detection and replacement of failed connections

#### `RedisTableOperations` Trait
All Redis table types implement this unified interface providing:
- **`load_data()`**: Unified data loading with optional pushdown conditions
- **`data_len()`**: Length calculation with optional filtered data support
- **`get_row()`**: Row retrieval with optional filtered data support
- **`insert()`**, **`delete()`**: CRUD operations (UPDATE not supported)
- **`supports_pushdown()`**: Pushdown capability checking

#### Table Type Implementations
Each Redis data type has its own specialized implementation:
- **`RedisHashTable`**: Optimized for HGET/HMGET operations
- **`RedisListTable`**: Handles ordered element collections
- **`RedisSetTable`**: Optimized for SISMEMBER operations
- **`RedisStringTable`**: Direct value access optimization
- **`RedisZsetTable`**: Scored set operations

#### Unified Method Interface
The refactoring consolidated similar methods:
```rust
// Before: Multiple similar methods
load_data_with_pushdown() + load_data()
get_row_from_filtered_data() + get_row()  
filtered_data_len() + data_len()

// After: Unified methods with optional parameters
load_data(conditions: Option<&[PushableCondition]>)
get_row(index: usize, filtered_data: Option<&[String]>)
data_len(filtered_data: Option<&[String]>)
```

#### State Management Simplification
The `RedisFdwState` now uses clean delegation:
```rust
// Simplified delegation pattern
match &mut self.table_type {
    RedisTableType::Hash(table) => table.load_data(conn, key_prefix, conditions),
    RedisTableType::List(table) => table.load_data(conn, key_prefix, conditions),
    // ... etc
}
```

## Performance Considerations

- **Memory Management**: The extension uses PostgreSQL's memory contexts for efficient memory allocation
- **Connection Pooling**: Integrated R2D2 connection pooling provides:
  - **Efficient connection reuse**: Eliminates overhead of creating new Redis connections for each operation
  - **Configurable pool sizing**: Customizable maximum connections (default: 10) and minimum idle connections
  - **Connection health monitoring**: Automatic validation and cleanup of stale connections
  - **Connection timeouts**: Configurable connection acquisition timeout (default: 30 seconds)
  - **Resource management**: Automatic connection lifecycle management with idle timeout (default: 10 minutes) and maximum lifetime (default: 30 minutes)
- **WHERE Clause Pushdown**: Supported conditions are executed directly in Redis for optimal performance
  - Hash tables: `field = 'value'` and `field IN (...)` use `HGET`/`HMGET`
  - Set tables: `member = 'value'` uses `SISMEMBER` for direct membership testing
  - String tables: `value = 'text'` avoids unnecessary data transfer
- **Data Loading**: Optimized data loading based on query conditions (pushdown when possible, full scan when necessary)
- **Filtering**: Non-pushable WHERE clauses are evaluated at PostgreSQL level after optimal Redis data retrieval
- **Insert Performance**: Uses Redis batch operations (`HSET` for multiple hash fields, `RPUSH` for lists)

### Query Performance Examples

```sql
-- Optimized: Uses HGET instead of HGETALL + filtering
SELECT value FROM user_profiles WHERE field = 'email';

-- Optimized: Uses HMGET for multiple fields
SELECT * FROM user_profiles WHERE field IN ('name', 'email', 'phone');

-- Optimized: Uses SISMEMBER for direct membership check
SELECT EXISTS(SELECT 1 FROM user_roles WHERE member = 'admin');
```

## Troubleshooting

### Common Issues

1. **Connection refused**: Verify Redis server is running and accessible
   ```bash
   redis-cli -h 127.0.0.1 -p 8899 ping
   ```

2. **Extension not found**: Ensure the extension is properly installed
   ```sql
   SELECT * FROM pg_available_extensions WHERE name = 'redis_fdw_rs';
   ```

3. **Permission denied**: Check PostgreSQL superuser privileges for extension creation

4. **Table options missing**: Ensure required options are specified
   - `host_port` (server option)
   - `table_type` (table option) 
   - `table_key_prefix` (table option)

5. **Unsupported table type**: All Redis table types (`hash`, `list`, `set`, `zset`, `string`) are supported for SELECT and INSERT operations

6. **UPDATE operations failing**: UPDATE operations are not supported by design and will not work with Redis FDW

7. **Connection pool exhaustion**: If you encounter connection timeout errors:
   - Check Redis server connection limits (`redis-cli CONFIG GET maxclients`)
   - Monitor connection pool usage in PostgreSQL logs
   - Consider increasing Redis server connection limits if needed
   - Default pool size is 10 connections per PostgreSQL backend

8. **Connection timeouts**: If operations timeout frequently:
   - Verify network connectivity to Redis server
   - Check Redis server response times
   - Monitor PostgreSQL logs for connection pool events
   - Consider adjusting connection timeout settings if needed

### Connection Pool Monitoring

Monitor connection pool health through PostgreSQL logs:
```
# Look for connection pool events in PostgreSQL logs
grep "redis_fdw" /var/log/postgresql/postgresql-*.log
```

### Debug Logging

Enable detailed logging by setting in `postgresql.conf`:
```
log_min_messages = debug1
```

Look for log messages starting with `---> redis_fdw` to trace execution.

## Testing

### Regular Testing

Run the comprehensive test suite:

```bash
# Run all tests for PostgreSQL 14
cargo pgrx test pg14

# Run tests for other PostgreSQL versions
cargo pgrx test pg15
cargo pgrx test pg16
cargo pgrx test pg17

# Run tests with a specific Redis server
REDIS_HOST_PORT="127.0.0.1:8899" cargo pgrx test pg14
```

### Performance and Load Testing

The project includes dedicated performance tests in the `load_tests/` directory:

```bash
# Set up load testing environment
cd load_tests
./setup_load_test.sh

# Run specific performance tests
# These SQL scripts test various scenarios:
# - hash_table_test.sql: Hash table performance
# - list_table_test.sql: List operations performance  
# - set_table_test.sql: Set operations performance
# - string_table_test.sql: String operations performance
# - zset_table_test.sql: Sorted set performance
# - mixed_operations_test.sql: Mixed workload testing
# - read_heavy_test.sql: Read-intensive scenarios
# - write_heavy_test.sql: Write-intensive scenarios
```

### Redis Cluster Testing

This project includes comprehensive Redis cluster integration testing infrastructure:

#### Quick Start with Cluster Testing

```bash
# 1. Start a Redis cluster using Docker Compose
./scripts/cluster_test.sh start

# 2. Run integration tests against the cluster
./scripts/cluster_test.sh test

# 3. Monitor cluster status
./scripts/cluster_test.sh status

# 4. View cluster logs
./scripts/cluster_test.sh logs

# 5. Clean up when done
./scripts/cluster_test.sh cleanup
```

#### Cluster Test Features

- **Automated Setup**: Docker Compose creates a 6-node Redis cluster (3 masters + 3 replicas)
- **Health Checks**: Ensures all nodes are ready before running tests
- **Complete Coverage**: Tests all Redis table types with cluster distribution
- **Error Handling**: Validates cluster resilience and failover scenarios
- **Performance Testing**: Verifies key distribution across cluster nodes

#### Manual Cluster Testing

```bash
# Set environment variables for cluster testing
export REDIS_CLUSTER_TEST_ENABLED=true
export REDIS_CLUSTER_NODES="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006"

# Run tests with cluster configuration
cargo pgrx test pg14
```

## Roadmap

### Recently Completed ✅
- ✅ **Redis Cluster support** with automatic failover and sharding
- ✅ **Object-oriented architecture refactoring** with unified trait interface
- ✅ **Method consolidation** eliminating duplicate functionality 
- ✅ **Enhanced encapsulation** with table-specific optimization logic
- ✅ **Simplified state management** using clean delegation patterns
- ✅ **Connection pooling and reuse** with R2D2 for improved performance and resource management
- ✅ Code restructuring and modular architecture
- ✅ Enhanced error handling for DELETE operations
- ✅ Implementation of SELECT and INSERT operations for all Redis data types
- ✅ DELETE operations for Hash, Set, ZSet, and String data types
- ✅ Improved memory safety and validation
- ✅ **WHERE clause pushdown optimization** for Hash, Set, and String table types

### Planned Features
- 🚧 Async operations support
- 🚧 Custom connection-pool on option.
- 🚧 Transaction support and rollback capabilities

**Note**: UPDATE operations are intentionally not supported due to fundamental differences between Redis data models and SQL UPDATE semantics. Redis operations like HSET, SADD, etc. are inherently insert-or-update operations, making traditional SQL UPDATE behavior problematic.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the terms specified in the LICENSE file.

## Supported PostgreSQL Versions

- PostgreSQL 14
- PostgreSQL 15  
- PostgreSQL 16
- PostgreSQL 17

## Dependencies

- **pgrx**: PostgreSQL extension framework for Rust
- **redis**: Redis client library for Rust with integrated R2D2 connection pooling support
- **r2d2**: Generic connection pool library providing efficient connection management and reuse