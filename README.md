# Redis Foreign Data Wrapper for PostgreSQL (Rust)

A high-performance Redis Foreign Data Wrapper (FDW) for PostgreSQL written in Rust using [pgrx](https://github.com/pgcentralfoundation/pgrx). Query and manipulate Redis data as regular PostgreSQL tables.

## Features

- **All Redis types**: Hash, List, Set, ZSet, String, Stream (SELECT/INSERT/UPDATE/DELETE; Stream is append-only)
- **Redis Cluster**: Automatic failover and sharding across multiple nodes
- **TLS/SSL**: `rediss://` URI scheme with rustls backend (no OpenSSL dependency)
- **Connection pooling**: R2D2 with configurable pool size, timeouts, and health checks
- **WHERE pushdown**: Conditions executed directly in Redis (HGET/HMGET, SISMEMBER, etc.)
- **TTL support**: Table-level default + per-row override via virtual `ttl` column
- **Multi-key patterns**: Glob patterns (`*`, `?`, `[`) in `table_key_prefix` to query multiple keys
- **DDL validation**: Option validator checks all options at CREATE time; column count validated at query time
- **Parameterized JOINs**: Point-lookup optimization for FDW-to-local JOINs (HGET, SISMEMBER, ZSCORE)
- **Stream pagination**: Configurable batch processing for large data sets
- **EXPLAIN support**: Detailed scan/modify metadata (server, key, pushdown, batch size)
- **Batch INSERT**: Pipelined multi-row inserts via `ExecForeignBatchInsert` (configurable `batch_size`)
- **TRUNCATE**: Single-key `UNLINK` or pattern-based `SCAN + UNLINK` for multi-key tables
- **IMPORT FOREIGN SCHEMA**: Auto-discovers Redis keys, groups by prefix, and generates DDL
- **ANALYZE**: Statistics gathering for query planner via type-specific cardinality commands
- **COPY FROM**: Bulk loading via `COPY` and `INSERT INTO ... SELECT` into foreign tables
- **JOIN support**: FDW-to-FDW join pushdown for same-server tables with automatic join column detection
- **OOM protection**: Soft limits with warnings for large datasets and connection pool saturation
- **Compatible with PostgreSQL 14-18**

## Prerequisites

- PostgreSQL 14, 15, 16, 17 or 18
- Redis server
- Rust toolchain (for building from source)

## Installation

### Install via APT (Recommended)

**Quick Install** (auto-detects PostgreSQL version):

```bash
curl -fsSL https://isdaniel.github.io/redis_fdw_rs/install.sh | sudo bash
```

**Manual Install:**

```bash
# Add GPG key
curl -fsSL https://isdaniel.github.io/redis_fdw_rs/gpg.key | \
  sudo gpg --dearmor -o /etc/apt/keyrings/redis-fdw-rs.gpg

# Add repository (auto-detects your distro)
echo "deb [signed-by=/etc/apt/keyrings/redis-fdw-rs.gpg] \
  https://isdaniel.github.io/redis_fdw_rs $(. /etc/os-release && echo $VERSION_CODENAME) main" | \
  sudo tee /etc/apt/sources.list.d/redis-fdw-rs.list

# Install (replace 16 with your PostgreSQL version)
sudo apt update
sudo apt install postgresql-16-redis-fdw-rs
```

**Supported Platforms:**

| OS | Codename | Architectures |
|----|----------|---------------|
| Ubuntu 22.04 | jammy | amd64 |
| Ubuntu 24.04 | noble | amd64 |
| Debian 11 | bullseye | amd64 |
| Debian 12 | bookworm | amd64 |

### Build from Source

```bash
# Start Redis
docker run -d --name redis-server -p 8899:6379 redis

# Build and install
cargo pgrx install --release

# Run interactive PG session
cargo pgrx run
```

```sql
CREATE EXTENSION redis_fdw_rs;
```

## Quick Start

```sql
-- 1. Create the FDW and server
CREATE FOREIGN DATA WRAPPER redis_wrapper
HANDLER redis_fdw_handler
VALIDATOR redis_fdw_validator;

CREATE SERVER redis_server
FOREIGN DATA WRAPPER redis_wrapper
OPTIONS (host_port '127.0.0.1:6379');

-- 2. Create foreign tables
CREATE FOREIGN TABLE user_profiles (key text, value text)
SERVER redis_server
OPTIONS (database '0', table_type 'hash', table_key_prefix 'user:profiles');

CREATE FOREIGN TABLE task_queue (element text)
SERVER redis_server
OPTIONS (database '0', table_type 'list', table_key_prefix 'tasks:pending');

-- 3. Use them like regular tables
INSERT INTO user_profiles VALUES ('name', 'John Doe'), ('email', 'john@example.com');
SELECT * FROM user_profiles;

INSERT INTO task_queue VALUES ('Process invoice #123');
SELECT * FROM task_queue;
```

## Table Types

| Type | Columns | Redis Commands | UPDATE |
|------|---------|----------------|--------|
| `string` | value | SET, GET, DEL | Yes |
| `hash` | field, value | HSET, HGETALL, HDEL | Yes |
| `list` | element | RPUSH, LRANGE, LREM, LSET | Yes |
| `set` | member | SADD, SMEMBERS, SREM | Yes |
| `zset` | member, score | ZADD, ZRANGE, ZREM | Yes |
| `stream` | stream_id, field1, value1, ... | XADD, XRANGE, XDEL | No (append-only) |

| Type    | SELECT | INSERT | UPDATE | DELETE | TRUNCATE |
|---------|--------|--------|--------|--------|----------|
| String  | ✅     | ✅     | ✅     | ✅     | ✅       |
| Hash    | ✅     | ✅     | ✅     | ✅     | ✅       |
| List    | ✅     | ✅     | ✅     | ✅     | ✅       |
| Set     | ✅     | ✅     | ✅     | ✅     | ✅       |
| ZSet    | ✅     | ✅     | ✅     | ✅     | ✅       |
| Stream  | ✅     | ✅     | ❌     | ✅     | ✅       |

### Table Definitions

```sql
CREATE FOREIGN TABLE redis_string (value TEXT)
SERVER redis_server OPTIONS (table_type 'string', table_key_prefix 'config:app_name');

CREATE FOREIGN TABLE redis_hash (field TEXT, value TEXT)
SERVER redis_server OPTIONS (table_type 'hash', table_key_prefix 'user:1');

CREATE FOREIGN TABLE redis_list (element TEXT)
SERVER redis_server OPTIONS (table_type 'list', table_key_prefix 'items');

CREATE FOREIGN TABLE redis_set (member TEXT)
SERVER redis_server OPTIONS (table_type 'set', table_key_prefix 'tags');

CREATE FOREIGN TABLE redis_zset (member TEXT, score FLOAT8)
SERVER redis_server OPTIONS (table_type 'zset', table_key_prefix 'leaderboard');

CREATE FOREIGN TABLE redis_stream (stream_id TEXT, event_type TEXT, event_data TEXT)
SERVER redis_server OPTIONS (table_type 'stream', table_key_prefix 'events');
```

### Column Constraints

Each Redis type enforces a specific number of data columns. The FDW validates this at first query time and raises a clear error if the table definition is invalid:

| Type    | Min Cols | Max Cols | Expected Columns                      |
|---------|----------|----------|---------------------------------------|
| string  | 1        | 1        | `value`                               |
| hash    | 2        | 2        | `field, value`                        |
| list    | 1        | 2        | `element` or `index, element`         |
| set     | 1        | 1        | `member`                              |
| zset    | 2        | 2        | `member, score`                       |
| stream  | 2        | ∞        | `stream_id, field1[, field2, ...]`    |

- **Multi-key mode** (`table_key_prefix` with glob): adds +1 for the key column (first column)
- **TTL column**: an optional `ttl bigint` column is automatically excluded from validation
- Validation occurs in `begin_foreign_scan` / `begin_foreign_modify` (first SELECT/INSERT/UPDATE/DELETE)

### Operations

```sql
-- String
INSERT INTO redis_string VALUES ('MyApp');
UPDATE redis_string SET value = 'NewName';

-- Hash
INSERT INTO redis_hash VALUES ('name', 'John'), ('age', '30');
SELECT * FROM redis_hash WHERE field = 'name';        -- pushdown: HGET
SELECT * FROM redis_hash WHERE field IN ('name','age'); -- pushdown: HMGET

-- Set
INSERT INTO redis_set VALUES ('red'), ('green'), ('blue');
UPDATE redis_set SET member = 'yellow' WHERE member = 'red';

-- Sorted Set
INSERT INTO redis_zset VALUES ('player1', 100.5), ('player2', 95.0);
SELECT * FROM redis_zset ORDER BY score DESC;
UPDATE redis_zset SET score = '200' WHERE member = 'player1';

-- Stream
INSERT INTO redis_stream VALUES ('*', 'user_login', '{"user":"123"}');
SELECT * FROM redis_stream WHERE stream_id >= '1640995200000-0';
```

## Configuration

### Server Options

```sql
CREATE SERVER redis_server
FOREIGN DATA WRAPPER redis_wrapper
OPTIONS (
    host_port '127.0.0.1:6379',           -- Required
    password 'your_password',              -- Optional
    pool_max_size '128',                   -- Max connections (1-512, default: 64)
    pool_min_idle '16',                    -- Min idle connections (default: 8)
    pool_connection_timeout_ms '10000',    -- Timeout ms (100-60000, default: 30000)
    pool_max_lifetime_secs '3600',         -- Max lifetime (60-7200, default: 1800)
    pool_idle_timeout_secs '300'           -- Idle timeout (30-3600, default: 600)
);
```

### Table Options

| Option | Required | Description |
|--------|----------|-------------|
| `table_type` | Yes | `string`, `hash`, `list`, `set`, `zset`, `stream` |
| `table_key_prefix` | Yes | Redis key or glob pattern for multi-key mode |
| `database` | No | Redis database number (0-15, default: 0) |
| `ttl` | No | Default key expiration in seconds |
| `batch_size` | No | Max rows per batch INSERT pipeline (100-100000, default: 5000) |

### Redis Cluster

Specify multiple nodes with comma-separated addresses:

```sql
CREATE SERVER redis_cluster
FOREIGN DATA WRAPPER redis_wrapper
OPTIONS (
    host_port '127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002',
    password 'your_password'
);
```

### TLS/SSL

| Scheme | Behavior |
|--------|----------|
| `host:port` | Plaintext TCP |
| `redis://host:port` | Plaintext TCP (explicit) |
| `rediss://host:port` | TLS with certificate verification |
| `rediss://host:port/#insecure` | TLS without certificate verification |

```sql
-- TLS with verification
CREATE SERVER redis_secure
FOREIGN DATA WRAPPER redis_wrapper
OPTIONS (host_port 'rediss://redis.cloud.example.com:6380');

-- TLS without verification (dev/self-signed certs)
CREATE SERVER redis_dev
FOREIGN DATA WRAPPER redis_wrapper
OPTIONS (host_port 'rediss://redis-dev.internal:6380/#insecure');

-- Cluster with TLS
CREATE SERVER redis_cluster_tls
FOREIGN DATA WRAPPER redis_wrapper
OPTIONS (host_port 'rediss://node1:6380,rediss://node2:6380,rediss://node3:6380');
```

### TTL Support

```sql
-- Table-level default TTL
CREATE FOREIGN TABLE cached_data (value text)
SERVER redis_server
OPTIONS (table_type 'string', table_key_prefix 'cache:item1', ttl '3600');

-- Per-row TTL override via virtual column
CREATE FOREIGN TABLE cached_items (value text, ttl bigint)
SERVER redis_server
OPTIONS (table_type 'string', table_key_prefix 'cache:item2', ttl '3600');

INSERT INTO cached_items VALUES ('short-lived', 60);   -- expires in 60s
UPDATE cached_items SET value = 'permanent', ttl = -1; -- persist forever
SELECT value, ttl FROM cached_items;                   -- shows remaining TTL
```

### Multi-Key Pattern Queries

When `table_key_prefix` contains glob characters (`*`, `?`, `[`), the FDW scans matching keys:

```sql
CREATE FOREIGN TABLE all_users (key text, value text)
SERVER redis_server
OPTIONS (table_type 'string', table_key_prefix 'user:*');

SELECT * FROM all_users;
-- key     | value
-- user:1  | alice
-- user:2  | bob

INSERT INTO all_users VALUES ('user:3', 'charlie');
DELETE FROM all_users WHERE key = 'user:2';
```

**Multi-key columns per type:**

| Table Type | Columns |
|-----------|---------|
| String | key, value |
| Hash | key, field, value |
| List | key, element |
| Set | key, member |
| ZSet | key, score, member |

## EXPLAIN Support

The FDW provides detailed information in `EXPLAIN` output for both scan and modify operations:

```sql
EXPLAIN (VERBOSE) SELECT * FROM user_profiles WHERE field = 'email';
```

## Batch INSERT

Multi-row INSERT statements are automatically pipelined to Redis for better throughput. Configure with the `batch_size` table option:

```sql
CREATE FOREIGN TABLE cached_items (key text, value text)
SERVER redis_server
OPTIONS (table_type 'hash', table_key_prefix 'cache:items', batch_size '5000');

-- All rows sent in a single Redis pipeline
INSERT INTO cached_items VALUES ('a', '1'), ('b', '2'), ('c', '3'), ...;
```

## TRUNCATE

`TRUNCATE` is supported for both single-key and multi-key pattern tables:

```sql
-- Single-key: UNLINKs the Redis key
TRUNCATE user_profiles;

-- Multi-key pattern: SCANs matching keys and UNLINKs them in batches
CREATE FOREIGN TABLE all_sessions (key text, value text)
SERVER redis_server OPTIONS (table_type 'string', table_key_prefix 'session:*');

TRUNCATE all_sessions;  -- Removes all session:* keys
```

## IMPORT FOREIGN SCHEMA

Auto-discover Redis keys and generate foreign table DDL:

```sql
-- Import all discovered tables
IMPORT FOREIGN SCHEMA "public"
FROM SERVER redis_server INTO my_schema;

-- Import only specific tables (matched by derived prefix name)
IMPORT FOREIGN SCHEMA "public" LIMIT TO (users, sessions)
FROM SERVER redis_server INTO my_schema;

-- Import all except specific tables
IMPORT FOREIGN SCHEMA "public" EXCEPT (temp_data)
FROM SERVER redis_server INTO my_schema;
```

The import process:
1. SCANs Redis keys (up to 10,000 samples)
2. TYPE-checks each key via pipeline
3. Groups keys by prefix (splits on `:` delimiter)
4. Generates `CREATE FOREIGN TABLE` DDL with appropriate columns per type

## Performance

### WHERE Pushdown

```sql
-- Hash: uses HGET instead of HGETALL + filtering
SELECT value FROM user_profiles WHERE field = 'email';

-- Hash: uses HMGET for multiple fields
SELECT * FROM user_profiles WHERE field IN ('name', 'email', 'phone');

-- Set: uses SISMEMBER for direct membership check
SELECT EXISTS(SELECT 1 FROM user_roles WHERE member = 'admin');
```

### Bulk Insert Example

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

## JOIN Support

### FDW-to-Local Table JOINs

Redis foreign tables can be joined with regular PostgreSQL tables. The FDW advertises parameterized paths for point-lookup columns, enabling O(1) per-row lookups instead of full rescans:

```sql
SELECT u.name, r.value
FROM users u
JOIN redis_hash_table r ON u.key = r.field;
```

**Parameterized path support** (O(1) per outer row):
- Hash: join on `field` column → HGET
- Set: join on `member` column → SISMEMBER
- ZSet: join on `member` column → ZSCORE
- String (multi-key): join on `key` column → GET

When the planner cannot use a parameterized path (e.g., join on a non-lookup column), it falls back to the standard nested-loop with full rescan.

### FDW-to-FDW JOINs (Same Server Pushdown)

When both sides of a JOIN are Redis foreign tables on the same server, the FDW pushes the join down — fetching both datasets in a single connection and performing an in-memory hash join:

```sql
-- Both tables on same Redis server → single-connection fetch + hash join
SELECT h.field, h.value, s.member
FROM redis_hash h
JOIN redis_set s ON h.field = s.member;
```

The FDW automatically detects which columns are used in the join condition from the query. Supported join types: INNER JOIN, LEFT JOIN.

**Limitations:**
- FDW-to-FDW pushdown requires both tables on the same Redis server (same `host_port`)
- Only single-column equality joins are pushed down (merge-joinable operators)
- Neither table can have base WHERE restrictions (these force nested-loop fallback)
- Multi-key pattern tables (glob in `table_key_prefix`) cannot be push-down joined
- Stream tables are not eligible for join pushdown (variable-width rows)
- RIGHT JOIN and FULL OUTER JOIN are not pushed down (handled via nested-loop)
- Both datasets are loaded into memory for the hash join (warning emitted if >500K rows)
- Redis command errors during join fetch are raised as SQL errors (no silent data loss)
- LEFT JOIN unmatched rows produce proper SQL NULLs (not string literals)

### Performance Tips for JOINs

- Use `EXPLAIN` to verify the planner chose an efficient strategy (look for "Foreign Scan" on the join relation)
- For large Redis datasets, add WHERE conditions to reduce data volume before the join
- FDW-to-FDW same-server joins avoid redundant connection overhead (single pooled connection for both datasets)
- The smaller dataset is always used as the hash-join build side for optimal memory usage

## Connection Pool Configuration

Configure pool behavior via server OPTIONS:

| Option | Default | Range | Description |
|--------|---------|-------|-------------|
| `pool_max_size` | 16 | 1-512 | Maximum connections in pool |
| `pool_min_idle` | 1 | 0-max_size | Minimum idle connections |
| `pool_connection_timeout_ms` | 30000 | 100-60000 | Timeout to acquire connection |
| `pool_max_lifetime_secs` | 1800 | 60-7200 | Max connection lifetime |
| `pool_idle_timeout_secs` | 600 | 30-3600 | Idle connection timeout |

Example:
```sql
CREATE SERVER redis_server FOREIGN DATA WRAPPER redis_fdw
OPTIONS (
    host_port '127.0.0.1:6379',
    pool_max_size '32',
    pool_min_idle '4'
);
```

## Development

### Building

```bash
cargo install --locked cargo-pgrx --version 0.18.0
cargo pgrx init
cargo pgrx install --release
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Connection refused | Verify Redis: `redis-cli -h 127.0.0.1 -p 6379 ping` |
| Extension not found | `SELECT * FROM pg_available_extensions WHERE name = 'redis_fdw_rs'` |
| Stream UPDATE fails | Streams are append-only; use INSERT + DELETE instead |
| Pool exhaustion | Increase `pool_max_size` or check `redis-cli CONFIG GET maxclients` |
| Connection timeouts | Adjust `pool_connection_timeout_ms`, verify network connectivity |

Enable debug logging in `postgresql.conf`:
```
log_min_messages = debug1
```

## License

This project is licensed under the terms specified in the LICENSE file.

## Supported PostgreSQL Versions

- PostgreSQL 14
- PostgreSQL 15  
- PostgreSQL 16
- PostgreSQL 17
- PostgreSQL 18

## Dependencies

- **pgrx**: PostgreSQL extension framework for Rust
- **redis**: Redis client library for Rust with integrated R2D2 connection pooling and TLS support
- **r2d2**: Generic connection pool library providing efficient connection management and reuse
- **rustls**: TLS implementation (pulled in via redis crate's `tls-rustls` feature)