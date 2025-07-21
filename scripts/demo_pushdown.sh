#!/bin/bash

# Redis FDW WHERE Clause Pushdown Demo
# This script demonstrates the performance benefits of WHERE clause pushdown

set -e

echo "=== Redis FDW WHERE Clause Pushdown Demo ==="
echo

# Check if Redis is running
if ! redis-cli ping > /dev/null 2>&1; then
    echo "❌ Redis is not running. Please start Redis first:"
    echo "   docker run -d --name redis-demo -p 6379:6379 redis"
    exit 1
fi

echo "✅ Redis is running"

# Connect to PostgreSQL and run demo
psql -h 127.0.0.1 -p 28814 -U azureuser -d postgres <<EOF
-- Clean up any existing demo tables
DROP FOREIGN TABLE IF EXISTS user_profile_pushdown CASCADE;
DROP FOREIGN TABLE IF EXISTS user_tags_pushdown CASCADE;
DROP SERVER IF EXISTS redis_pushdown_server CASCADE;
DROP FOREIGN DATA WRAPPER IF EXISTS redis_pushdown_wrapper CASCADE;

-- Create the FDW
CREATE FOREIGN DATA WRAPPER redis_pushdown_wrapper 
HANDLER redis_fdw_handler;

-- Create server
CREATE SERVER redis_pushdown_server 
FOREIGN DATA WRAPPER redis_pushdown_wrapper
OPTIONS (host_port '127.0.0.1:6379');

-- Create hash table for user profile
CREATE FOREIGN TABLE user_profile_pushdown (field text, value text) 
SERVER redis_pushdown_server
OPTIONS (
    database '0',
    table_type 'hash',
    table_key_prefix 'demo:user:123'
);

-- Create set table for user tags
CREATE FOREIGN TABLE user_tags_pushdown (member text)
SERVER redis_pushdown_server
OPTIONS (
    database '0', 
    table_type 'set',
    table_key_prefix 'demo:user:123:tags'
);

-- Insert test data
INSERT INTO user_profile_pushdown VALUES 
    ('name', 'John Doe'),
    ('email', 'john@example.com'),
    ('age', '30'),
    ('department', 'Engineering'),
    ('city', 'San Francisco'),
    ('phone', '+1-555-0123'),
    ('title', 'Senior Developer'),
    ('manager', 'Jane Smith'),
    ('start_date', '2020-01-15'),
    ('salary', '95000');

INSERT INTO user_tags_pushdown VALUES 
    ('developer'),
    ('rust'),
    ('postgresql'),
    ('senior'),
    ('backend'),
    ('cloud'),
    ('microservices'),
    ('api');

\timing on
SELECT value FROM user_profile_pushdown WHERE field = 'email';

SELECT field, value FROM user_profile_pushdown 
WHERE field IN ('name', 'email', 'department');

SELECT EXISTS(SELECT 1 FROM user_tags_pushdown WHERE member = 'rust');

SELECT COUNT(*) FROM user_tags_pushdown 
WHERE member IN ('rust', 'python', 'go', 'javascript');

SELECT field, value FROM user_profile_pushdown 
WHERE field LIKE '%name%' AND value != 'N/A';

SELECT COUNT(*) FROM user_profile_pushdown;

SELECT value FROM user_profile_pushdown WHERE field = 'salary';

DROP FOREIGN TABLE user_profile_pushdown;
DROP FOREIGN TABLE user_tags_pushdown;
DROP SERVER redis_pushdown_server CASCADE;
DROP FOREIGN DATA WRAPPER redis_pushdown_wrapper CASCADE;
\timing off
EOF
