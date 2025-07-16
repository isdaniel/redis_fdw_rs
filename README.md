# Redis Foreign Data Wrapper for PostgreSQL (Rust)

A high-performance Redis Foreign Data Wrapper (FDW) for PostgreSQL written in Rust using the [pgrx](https://github.com/pgcentralfoundation/pgrx) framework. This extension allows PostgreSQL to directly query and manipulate Redis data as if it were regular PostgreSQL tables.

## Features

- **High-performance data access** from Redis to PostgreSQL
- **Redis data types support**: Hash (fully implemented), List (fully implemented)
- **Supported operations**: SELECT, INSERT (UPDATE and DELETE planned for future releases)
- **Connection management** and memory optimization
- **Built with Rust** for memory safety and performance
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
CREATE SERVER redis_server 
FOREIGN DATA WRAPPER redis_wrapper
OPTIONS (
    host_port '127.0.0.1:8899'
);
```

### 3. Create User Mapping (Optional)
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
  - `'string'` - Partal implemented ✅
  - `'hash'` - Partial implemented ✅
  - `'list'` - Partial implemented ✅  
  - `'set'` - Partial implemented ✅
  - `'zset'` - Partial implemented ✅
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

## Current Limitations

- **UPDATE operations**: Not yet implemented (will return successfully but no changes made)
- **DELETE operations**: Not yet implemented (will return successfully but no changes made)
- **Set, ZSet, String types**: Defined but not implemented
- **Transactions**: Redis operations are not transactional with PostgreSQL
- **Complex WHERE clauses**: Filtering happens at PostgreSQL level, not pushed down to Redis

## Development

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
docker run -d --name redis-test -p 6379:6379 redis
cargo pgrx test --features integration_tests
docker stop redis-test && docker rm redis-test

# Test specific PostgreSQL version
cargo pgrx test pg16
```

See [TESTING.md](TESTING.md) for detailed testing documentation.

## Performance Considerations

- **Memory Management**: The extension uses PostgreSQL's memory contexts for efficient memory allocation
- **Connection Management**: Redis connections are established per query execution
- **Data Loading**: All data for a table is loaded at scan initialization (not suitable for very large Redis keys)
- **Filtering**: WHERE clauses are evaluated at PostgreSQL level, not pushed down to Redis
- **Insert Performance**: Uses Redis batch operations (`HSET` for multiple hash fields, `RPUSH` for lists)

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

5. **Unsupported table type**: Currently only `'hash'` and `'list'` are supported

6. **UPDATE/DELETE not working**: These operations are not yet implemented

### Debug Logging

Enable detailed logging by setting in `postgresql.conf`:
```
log_min_messages = debug1
```

Look for log messages starting with `---> redis_fdw` to trace execution.

## Roadmap

### Planned Features
- 🚧 UPDATE and DELETE operations for Hash and List types
- 🚧 Set data type support
- 🚧 Sorted Set (ZSet) data type support  
- 🚧 String data type support
- 🚧 WHERE clause pushdown to Redis
- 🚧 Connection pooling
- 🚧 Async operations support
- 🚧 Redis Cluster support
- 🚧 Better error handling and recovery

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