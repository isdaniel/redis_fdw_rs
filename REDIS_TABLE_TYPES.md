# Redis FDW Table Types - Object-Oriented Design

This document describes the new object-oriented design for Redis table types in the Redis FDW extension.

## Overview

The new design uses a trait-based approach where each Redis data type is implemented as a separate struct with common operations defined by the `RedisTableOperations` trait.

## Architecture

### Trait Definition

```rust
pub trait RedisTableOperations {
    fn load_data(&mut self, conn: &mut redis::Connection, key_prefix: &str) -> Result<(), redis::RedisError>;
    fn data_len(&self) -> usize;
    fn get_row(&self, index: usize) -> Option<Vec<String>>;
    fn insert(&mut self, conn: &mut redis::Connection, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError>;
    fn delete(&mut self, conn: &mut redis::Connection, key_prefix: &str, data: &[String]) -> Result<(), redis::RedisError>;
    fn update(&mut self, conn: &mut redis::Connection, key_prefix: &str, old_data: &[String], new_data: &[String]) -> Result<(), redis::RedisError>;
}
```

### Redis Table Types

#### 1. RedisStringTable
- **Purpose**: Handle Redis STRING operations
- **Data Format**: Single string value
- **SQL Columns**: 1 column (value)

```rust
pub struct RedisStringTable {
    pub data: Option<String>,
}
```

**Operations:**
- `INSERT`: Sets the string value
- `DELETE`: Deletes the key
- `UPDATE`: Replaces the string value
- `SELECT`: Returns the string value

#### 2. RedisHashTable
- **Purpose**: Handle Redis HASH operations
- **Data Format**: Key-value pairs
- **SQL Columns**: 2 columns (field, value)

```rust
pub struct RedisHashTable {
    pub data: Vec<(String, String)>,
}
```

**Operations:**
- `INSERT`: Uses HSET to add/update field-value pairs
- `DELETE`: Uses HDEL to remove specific fields
- `UPDATE`: Overwrites existing field-value pairs
- `SELECT`: Returns all field-value pairs

#### 3. RedisListTable
- **Purpose**: Handle Redis LIST operations
- **Data Format**: Ordered list of strings
- **SQL Columns**: 1 column (element)

```rust
pub struct RedisListTable {
    pub data: Vec<String>,
}
```

**Operations:**
- `INSERT`: Uses RPUSH to append elements
- `DELETE`: Uses LREM to remove elements
- `UPDATE`: Removes old values and inserts new ones
- `SELECT`: Returns all list elements in order

#### 4. RedisSetTable
- **Purpose**: Handle Redis SET operations
- **Data Format**: Unordered set of unique strings
- **SQL Columns**: 1 column (member)

```rust
pub struct RedisSetTable {
    pub data: Vec<String>,
}
```

**Operations:**
- `INSERT`: Uses SADD to add members
- `DELETE`: Uses SREM to remove members
- `UPDATE`: Removes old members and adds new ones
- `SELECT`: Returns all set members

#### 5. RedisZSetTable
- **Purpose**: Handle Redis SORTED SET operations
- **Data Format**: Ordered set with scores
- **SQL Columns**: 2 columns (member, score)

```rust
pub struct RedisZSetTable {
    pub data: Vec<(String, f64)>,
}
```

**Operations:**
- `INSERT`: Uses ZADD to add member-score pairs
- `DELETE`: Uses ZREM to remove members
- `UPDATE`: Removes old members and adds new ones
- `SELECT`: Returns members with their scores

## Usage Examples

### Creating Foreign Tables

#### String Table
```sql
CREATE FOREIGN TABLE redis_string (
    value TEXT
) SERVER redis_server OPTIONS (
    table_type 'string',
    table_key_prefix 'mystring'
);
```

#### Hash Table
```sql
CREATE FOREIGN TABLE redis_hash (
    field TEXT,
    value TEXT
) SERVER redis_server OPTIONS (
    table_type 'hash',
    table_key_prefix 'myhash'
);
```

#### List Table
```sql
CREATE FOREIGN TABLE redis_list (
    element TEXT
) SERVER redis_server OPTIONS (
    table_type 'list',
    table_key_prefix 'mylist'
);
```

#### Set Table
```sql
CREATE FOREIGN TABLE redis_set (
    member TEXT
) SERVER redis_server OPTIONS (
    table_type 'set',
    table_key_prefix 'myset'
);
```

#### Sorted Set Table
```sql
CREATE FOREIGN TABLE redis_zset (
    member TEXT,
    score FLOAT8
) SERVER redis_server OPTIONS (
    table_type 'zset',
    table_key_prefix 'myzset'
);
```

### Operations Examples

#### Hash Table Operations
```sql
-- Insert field-value pairs
INSERT INTO redis_hash VALUES ('name', 'John'), ('age', '30');

-- Query all fields
SELECT * FROM redis_hash;

-- Delete specific fields
DELETE FROM redis_hash WHERE field = 'age';

-- Update field values
UPDATE redis_hash SET value = 'Jane' WHERE field = 'name';
```

#### List Table Operations
```sql
-- Append elements to list
INSERT INTO redis_list VALUES ('apple'), ('banana'), ('cherry');

-- Query all elements
SELECT * FROM redis_list;

-- Remove specific elements
DELETE FROM redis_list WHERE element = 'banana';
```

#### Set Table Operations
```sql
-- Add members to set
INSERT INTO redis_set VALUES ('red'), ('green'), ('blue');

-- Query all members
SELECT * FROM redis_set;

-- Remove members
DELETE FROM redis_set WHERE member = 'green';
```

#### Sorted Set Table Operations
```sql
-- Add members with scores
INSERT INTO redis_zset VALUES ('player1', 100.5), ('player2', 95.0);

-- Query all members with scores
SELECT * FROM redis_zset ORDER BY score DESC;

-- Remove members
DELETE FROM redis_zset WHERE member = 'player1';

-- Update scores
UPDATE redis_zset SET score = 98.0 WHERE member = 'player2';
```

## Benefits of Object-Oriented Design

1. **Separation of Concerns**: Each Redis data type has its own struct with specific logic
2. **Extensibility**: Easy to add new Redis data types by implementing the trait
3. **Maintainability**: Clear interface and implementation separation
4. **Type Safety**: Compile-time guarantees for operations
5. **Code Reuse**: Common operations defined in the trait can be reused
6. **Testing**: Each table type can be unit tested independently

## Implementation Details

### State Management
The `RedisFdwState` now uses the enum `RedisTableType` which contains the specific table type implementations:

```rust
pub enum RedisTableType {
    String(RedisStringTable),
    Hash(RedisHashTable),
    List(RedisListTable),
    Set(RedisSetTable),
    ZSet(RedisZSetTable),
    None,
}
```

### Unified Interface
All operations are now handled through unified methods in `RedisFdwState`:
- `insert_data(&mut self, data: &[String])`
- `delete_data(&mut self, data: &[String])`
- `update_data(&mut self, old_data: &[String], new_data: &[String])`
- `get_row(&self, index: usize)`

### Error Handling
The new design includes proper error handling using `Result<(), redis::RedisError>` for all Redis operations.

## Migration from Old Design

The old enum-based approach:
```rust
pub enum RedisTableType {
    String,
    Hash(Vec<(String, String)>),
    List(Vec<String>),
    Set,
    ZSet,
    None
}
```

Has been replaced with the new object-oriented approach that provides better encapsulation and extensibility.

Legacy methods are still available for backward compatibility but will use the new unified interface internally.
