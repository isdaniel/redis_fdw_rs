#!/bin/bash

# Quick setup script for Redis FDW load testing environment
# This script sets up the basic infrastructure needed for load testing

set -e

REDIS_HOST=${REDIS_HOST:-127.0.0.1}
REDIS_PORT=${REDIS_PORT:-8899}
PG_DATABASE=${PG_DATABASE:-postgres}
PG_USER=${PG_USER:-azureuser}
PG_HOST=${PG_HOST:-127.0.0.1}
PG_PORT=${PG_PORT:-28814} #replace with your PostgreSQL port

echo "🚀 Setting up Redis FDW Load Testing Environment"
echo "================================================="

# Function to execute SQL
execute_sql() {
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" -c "$1"
}

# Check if Redis is running
echo "Checking Redis server..."
if ! redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping > /dev/null 2>&1; then
    echo "❌ Redis server is not running at $REDIS_HOST:$REDIS_PORT"
    echo "Starting Redis server with Docker..."
    if command -v docker &> /dev/null; then
        docker run -d --name redis-server -p "$REDIS_PORT":6379 redis
        echo "✅ Redis server started on port $REDIS_PORT"
        sleep 3
    else
        echo "Docker not found. Please start Redis manually:"
        echo "redis-server --port $REDIS_PORT"
        exit 1
    fi
else
    echo "✅ Redis server is running"
fi

# Setup PostgreSQL extension
echo "Setting up PostgreSQL extension..."
execute_sql "CREATE EXTENSION IF NOT EXISTS redis_fdw_rs;" || {
    echo "❌ Failed to create Redis FDW extension"
    echo "Make sure the extension is installed: cargo pgrx install --release"
    exit 1
}

# Create foreign data wrapper
echo "Creating foreign data wrapper..."
execute_sql "DROP FOREIGN DATA WRAPPER IF EXISTS redis_wrapper CASCADE;"
execute_sql "CREATE FOREIGN DATA WRAPPER redis_wrapper HANDLER redis_fdw_handler;"

# Create server
echo "Creating Redis server..."
execute_sql "CREATE SERVER redis_server FOREIGN DATA WRAPPER redis_wrapper OPTIONS (host_port '$REDIS_HOST:$REDIS_PORT');"

# Create foreign tables
echo "Creating foreign tables for load testing..."
execute_sql "
    -- Drop existing tables to ensure a clean state
    DROP FOREIGN TABLE IF EXISTS redis_string;
    DROP FOREIGN TABLE IF EXISTS redis_hash;
    DROP FOREIGN TABLE IF EXISTS redis_list;
    DROP FOREIGN TABLE IF EXISTS redis_set;
    DROP FOREIGN TABLE IF EXISTS redis_zset;

    -- Create Foreign Tables for pgbench tests
    CREATE FOREIGN TABLE redis_string (key TEXT, value TEXT)
        SERVER redis_server
        OPTIONS (
            database '0',
            table_type 'string',
            table_key_prefix 'pgbench:string:'
        );

    CREATE FOREIGN TABLE redis_hash (key TEXT, field TEXT, value TEXT)
        SERVER redis_server
        OPTIONS (
            database '0',
            table_type 'hash'
        );

    CREATE FOREIGN TABLE redis_list (key TEXT, element TEXT)
        SERVER redis_server
        OPTIONS (
            database '0',
            table_type 'list'
        );

    CREATE FOREIGN TABLE redis_set (key TEXT, member TEXT)
        SERVER redis_server
        OPTIONS (
            database '0',
            table_type 'set'
        );

    CREATE FOREIGN TABLE redis_zset (key TEXT, member TEXT, score FLOAT8)
        SERVER redis_server
        OPTIONS (
            database '0',
            table_type 'zset'
        );
"

# Test the setup
echo "Testing the setup..."
execute_sql "INSERT INTO redis_string (key, value) VALUES ('test:string:init', 'hello world');"
execute_sql "SELECT * FROM redis_string WHERE key = 'test:string:init';"
execute_sql "DELETE FROM redis_string WHERE key = 'test:string:init';"

echo ""
echo "🎉 Setup complete!"
echo "✅ Redis FDW extension is ready for load testing"
echo ""
echo "Quick test commands:"
echo "  # Run mixed operations test for 30 seconds with 10 clients"
echo "  pgbench -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DATABASE -c 10 -j 4 -T 30 -f ./load_tests/mixed_operations_test.sql"
echo ""
echo "  # Run the full test suite"
echo "  ./redis_fdw_load_test.sh"
echo ""
echo "  # Check data in Redis"
echo "  redis-cli -h $REDIS_HOST -p $REDIS_PORT keys '*'"
