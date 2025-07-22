# Redis Foreign Data Wrapper for PostgreSQL (Rust)

A high-performance Redis Foreign Data Wrapper (FDW) for PostgreSQL written in Rust using the [pgrx](https://github.com/pgcentralfoundation/pgrx) framework. This extension allows PostgreSQL to directly query and manipulate Redis data as if it were regular PostgreSQL tables.

## Features

- **High-performance data access** from Redis to PostgreSQL
- **Redis Cluster support** with automatic failover and sharding across multiple nodes
- **WHERE clause pushdown optimization** for significantly improved query performance
- **Redis data types support**: Hash, List, Set, ZSet, and String (with varying levels of implementation)
- **Supported operations**: SELECT, INSERT, UPDATE, DELETE (with improved error handling)
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
docker run -d --name redis-server -p 8899:6379 redis
```

2. **Build and install the extension:**
```bash
cargo pgrx install --release
```

3. **Create the extension in PostgreSQL:**
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
```

## Configuration

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
    host_port '127.0.0.1:8899'
);
```

### 3. Redis Cluster Support

Redis FDW supports both single-node and cluster deployments. To connect to a Redis cluster, specify multiple nodes in the `host_port` option using comma-separated addresses:

```sql
-- Redis Cluster server (automatic failover and sharding)
CREATE SERVER redis_cluster_server 
FOREIGN DATA WRAPPER redis_wrapper
OPTIONS (
    host_port '127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002'
);

-- You can mix IP addresses and hostnames
CREATE SERVER redis_cluster_prod 
FOREIGN DATA WRAPPER redis_wrapper
OPTIONS (
    host_port 'redis-node1.example.com:7000,redis-node2.example.com:7001,redis-node3.example.com:7002'
);

-- Minimal cluster setup (only one node needed for discovery)
CREATE SERVER redis_cluster_minimal 
FOREIGN DATA WRAPPER redis_wrapper
OPTIONS (
    host_port '10.0.0.100:7000'
);
```

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

### 4. Create User Mapping (Optional)
```sql
CREATE USER MAPPING FOR PUBLIC 
SERVER redis_server 
OPTIONS (password 'your_redis_password');
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
```

### SQL Operations
```sql
-- String operations
INSERT INTO redis_string VALUES ('MyApplicationName');
SELECT * FROM redis_string;
UPDATE redis_string SET value = 'UpdatedAppName';

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
```

## Configuration Options

### Server Options
- `host_port`: Redis connection string (format: `host:port`) - **Required**

### Table Options
- `database`: Redis database number (default: 0) - **Optional**
- `table_type`: Redis data type - **Required**
  - `'string'` - Partial implemented ✅ (SELECT, INSERT, DELETE; UPDATE not implemented)
  - `'hash'` - Partial implemented ✅ (SELECT, INSERT, DELETE; UPDATE not implemented)
  - `'list'` - Partial implemented ✅ (SELECT, INSERT DELETE; UPDATE not implemented)
  - `'set'` - Partial implemented ✅ (SELECT, INSERT, DELETE; UPDATE not implemented)
  - `'zset'` - Partial implemented ✅ (SELECT, INSERT, DELETE; UPDATE not implemented)
- `table_key_prefix`: Key prefix for Redis operations - **Required**

### User Mapping Options
- `password`: Redis authentication password - **Optional**

## Advanced Usage

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

-- Update configuration
UPDATE app_config SET value = 'postgresql://newhost:5432/mydb';
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
| Hash       | ✅     | ✅     | 🚧     | ✅     | **Partial** (UPDATE in progress) |
| List       | ✅     | ✅     | 🚧     | ✅     | **Partial** (UPDATE in progress) |
| Set        | ✅     | ✅     | 🚧     | ✅     | **Partial** (UPDATE in progress) |
| ZSet       | ✅     | ✅     | 🚧     | ✅     | **Partial** (UPDATE in progress) |
| String     | ✅     | ✅     | 🚧     | ✅     | **Partial** (UPDATE in progress) |

## Recent Changes (v0.3.0)

### 🏗️ Major Object-Oriented Refactoring
- **Complete architectural restructuring** with object-oriented design principles
- **Unified trait interface**: All Redis table types now implement a consistent `RedisTableOperations` trait
- **Method consolidation**: Eliminated duplicate methods by merging similar functionality:
  - `load_data_with_pushdown` + `load_data` → **unified `load_data`** (with optional pushdown conditions)
  - `get_row_from_filtered_data` + `get_row` → **unified `get_row`** (with optional filtered data)
  - `filtered_data_len` + `data_len` → **unified `data_len`** (with optional filtered data)

### Enhanced Code Organization
- **Encapsulated table-specific logic**: Each Redis table type now manages its own optimization strategies
- **Simplified state management**: The `state.rs` file now delegates to table implementations instead of containing type-specific logic
- **Consistent interface**: All table operations follow the same pattern with optional parameters for flexibility
- **Better maintainability**: Reduced code duplication and improved separation of concerns

### Performance Optimizations
- **Table-specific optimizations**: Each table type can implement its own optimization strategies:
  - Hash tables: HGET/HMGET optimizations for field-specific queries
  - Set tables: SISMEMBER for membership testing
  - String tables: Direct value access without unnecessary transfers
- **Unified pushdown logic**: Consolidated pushdown optimization handling across all table types
- **Memory efficiency**: Improved memory allocation and data handling patterns

## Previous Changes (v0.2.0)

### Code Restructuring
- **Modular Architecture**: Reorganized Redis table implementations into a dedicated `tables/` module
- **Improved Organization**: 
  - `src/redis_fdw/tables/` - Contains all table type implementations
  - `src/redis_fdw/tables/interface.rs` - Common trait definitions
  - `src/redis_fdw/tables/mod.rs` - Module exports and re-exports
- **Better Maintainability**: Cleaner separation of concerns between different Redis data types

### Enhanced Error Handling
- **Robust DELETE Operations**: Improved `exec_foreign_delete` function with comprehensive error handling
- **Key Validation**: Added proper validation for deletion keys (null checks, empty string handling)
- **Graceful Degradation**: Better error recovery to maintain PostgreSQL stability
- **Enhanced Logging**: More detailed logging for debugging and monitoring

### Technical Improvements
- **Memory Safety**: Enhanced unsafe code blocks with better validation
- **Code Quality**: Reduced code duplication and improved maintainability
- **Performance**: Optimized memory allocation and connection handling

## Current Limitations

- **UPDATE Operations**: UPDATE operations are not yet implemented for any Redis table type (returns `unimplemented!` error)
- **List Operations**: UPDATE and DELETE operations for List type are not yet fully implemented
- **Transactions**: Redis operations are not transactional with PostgreSQL
- **Complex WHERE clauses**: Filtering happens at PostgreSQL level, not pushed down to Redis
- **Large Data Sets**: All data for a table is loaded at scan initialization (not suitable for very large Redis keys)
- **Connection Pooling**: Each operation creates a new Redis connection (connection pooling planned)

## Development

### Project Structure
```
src/
├── redis_fdw/
│   ├── mod.rs                   # Updated module declarations
│   ├── handlers.rs              # PostgreSQL FDW handler functions (updated imports)
│   ├── pushdown.rs             # WHERE clause pushdown logic (types moved out)
│   ├── pushdown_types.rs       # 🆕 Pushdown condition types and analysis structures
│   ├── state.rs                # FDW state management (RedisTableType moved out)
│   ├── types.rs                # 🆕 Core Redis FDW data types and enums
│   ├── connection.rs           # 🆕 Redis connection management types
│   ├── table_type_tests.rs     # Unit tests for table types (updated imports)
│   ├── tests.rs                # Integration tests (updated imports)
│   ├── pushdown_tests.rs       # Pushdown tests (updated imports)
│   └── tables/                 # Redis table implementations (OOP architecture)
│       ├── mod.rs              # Table module exports
│       ├── interface.rs        # RedisTableOperations trait (RedisConnectionType moved out)
│       ├── redis_hash_table.rs # Hash table implementation (updated imports)
│       ├── redis_list_table.rs # List table implementation (updated imports)
│       ├── redis_set_table.rs  # Set table implementation (updated imports)
│       ├── redis_string_table.rs # String table implementation (updated imports)
│       └── redis_zset_table.rs # Sorted set implementation (updated imports)
└── utils_share/                # Shared utilities (unchanged)
    ├── cell.rs                 # Data cell types
    ├── memory.rs              # Memory management
    ├── row.rs                 # Row operations
    └── utils.rs               # General utilities
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

### Object-Oriented Design

The Redis FDW now follows a clean object-oriented architecture with the following key components:

#### `RedisTableOperations` Trait
All Redis table types implement this unified interface providing:
- **`load_data()`**: Unified data loading with optional pushdown conditions
- **`data_len()`**: Length calculation with optional filtered data support
- **`get_row()`**: Row retrieval with optional filtered data support
- **`insert()`**, **`delete()`**, **`update()`**: CRUD operations
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
- **Connection Management**: Redis connections are established per query execution
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

6. **UPDATE operations failing**: UPDATE operations are not yet implemented and will return an `unimplemented!` error

7. **DELETE operations failing on Lists**: DELETE operations for List type are not yet fully implemented

### Debug Logging

Enable detailed logging by setting in `postgresql.conf`:
```
log_min_messages = debug1
```

Look for log messages starting with `---> redis_fdw` to trace execution.

## Roadmap

### Recently Completed ✅
- ✅ **Redis Cluster support** with automatic failover and sharding
- ✅ **Object-oriented architecture refactoring** with unified trait interface
- ✅ **Method consolidation** eliminating duplicate functionality 
- ✅ **Enhanced encapsulation** with table-specific optimization logic
- ✅ **Simplified state management** using clean delegation patterns
- ✅ Code restructuring and modular architecture
- ✅ Enhanced error handling for DELETE operations
- ✅ Implementation of SELECT and INSERT operations for all Redis data types
- ✅ DELETE operations for Hash, Set, ZSet, and String data types
- ✅ Improved memory safety and validation
- ✅ **WHERE clause pushdown optimization** for Hash, Set, and String table types

### Planned Features
- 🚧 **UPDATE operations for all Redis table types** - Currently returns `unimplemented!` error
- 🚧 Complete DELETE operations for List types
- 🚧 Extended WHERE clause pushdown (ZSet score ranges, LIKE pattern optimization)
- 🚧 Connection pooling and reuse
- 🚧 Async operations support
- 🚧 Streaming support for large data sets
- 🚧 Advanced Redis operations (SCAN, pattern matching)
- 🚧 Transaction support and rollback capabilities

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
- **redis**: Redis client library for Rust