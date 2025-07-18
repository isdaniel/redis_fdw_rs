#!/bin/bash

# Redis FDW Load Testing Script with pgbench
# This script creates a comprehensive load testing environment for the Redis FDW extension

set -e

# Configuration
REDIS_HOST=${REDIS_HOST:-127.0.0.1}
REDIS_PORT=${REDIS_PORT:-8899}
PG_DATABASE=${PG_DATABASE:-postgres}
PG_USER=${PG_USER:-azureuser}
PG_HOST=${PG_HOST:-127.0.0.1}
PG_PORT=${PG_PORT:-28814}

# pgbench parameters
CLIENTS=${CLIENTS:-10}
JOBS=${JOBS:-4}
TRANSACTIONS=${TRANSACTIONS:-1000}
DURATION=${DURATION:-60}

echo "🚀 Redis FDW Load Testing Setup"
echo "================================="
echo "Redis: $REDIS_HOST:$REDIS_PORT"
echo "PostgreSQL: $PG_HOST:$PG_PORT/$PG_DATABASE"
echo "Clients: $CLIENTS, Jobs: $JOBS, Transactions: $TRANSACTIONS, Duration: ${DURATION}s"
echo ""

# Function to execute SQL
execute_sql() {
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" -c "$1"
}

# Function to check if Redis is running
check_redis() {
    if ! redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping > /dev/null 2>&1; then
        echo "❌ Redis server is not running at $REDIS_HOST:$REDIS_PORT"
        echo "Please start Redis server first:"
        echo "docker run -d --name redis-server -p $REDIS_PORT:6379 redis"
        exit 1
    fi
    echo "✅ Redis server is running"
}

# Function to setup the FDW extension
setup_fdw() {
    echo "Setting up Redis FDW extension..."
    
    # Create extension if not exists
    execute_sql "CREATE EXTENSION IF NOT EXISTS redis_fdw_rs;"
    
    # Create foreign data wrapper
    execute_sql "DROP FOREIGN DATA WRAPPER IF EXISTS redis_wrapper CASCADE;"
    execute_sql "CREATE FOREIGN DATA WRAPPER redis_wrapper HANDLER redis_fdw_handler;"
    
    # Create server
    execute_sql "CREATE SERVER redis_server FOREIGN DATA WRAPPER redis_wrapper OPTIONS (host_port '$REDIS_HOST:$REDIS_PORT');"
    
    echo "✅ Redis FDW extension setup complete"
}

# Function to create foreign tables
create_foreign_tables() {
    echo "Creating foreign tables for all Redis data types..."
    
    # String table
    execute_sql "DROP FOREIGN TABLE IF EXISTS redis_string_test CASCADE;"
    execute_sql "CREATE FOREIGN TABLE redis_string_test (value TEXT) SERVER redis_server OPTIONS (database '0', table_type 'string', table_key_prefix 'test:string');"
    
    # Hash table
    execute_sql "DROP FOREIGN TABLE IF EXISTS redis_hash_test CASCADE;"
    execute_sql "CREATE FOREIGN TABLE redis_hash_test (field TEXT, value TEXT) SERVER redis_server OPTIONS (database '0', table_type 'hash', table_key_prefix 'test:hash');"
    
    # List table
    execute_sql "DROP FOREIGN TABLE IF EXISTS redis_list_test CASCADE;"
    execute_sql "CREATE FOREIGN TABLE redis_list_test (element TEXT) SERVER redis_server OPTIONS (database '0', table_type 'list', table_key_prefix 'test:list');"
    
    # Set table
    execute_sql "DROP FOREIGN TABLE IF EXISTS redis_set_test CASCADE;"
    execute_sql "CREATE FOREIGN TABLE redis_set_test (member TEXT) SERVER redis_server OPTIONS (database '0', table_type 'set', table_key_prefix 'test:set');"
    
    # Sorted set table
    execute_sql "DROP FOREIGN TABLE IF EXISTS redis_zset_test CASCADE;"
    execute_sql "CREATE FOREIGN TABLE redis_zset_test (member TEXT, score FLOAT8) SERVER redis_server OPTIONS (database '0', table_type 'zset', table_key_prefix 'test:zset');"
    
    echo "✅ Foreign tables created successfully"
}

# Function to create pgbench custom scripts
create_pgbench_scripts() {
    echo "Creating pgbench test scripts..."
    
    # Test script 1: Mixed operations on different table types
    cat > pgbench_redis_mixed.sql << 'EOF'
-- Mixed operations test script
\set client_id random(1, 10000)
\set operation_type random(1, 15)

-- String operations (15% of operations)
\if :operation_type <= 2
    DELETE FROM redis_string_test;
    INSERT INTO redis_string_test VALUES ('client_' || :client_id || '_' || :operation_type);
    SELECT * FROM redis_string_test;
\endif

-- Hash operations (40% of operations)
\if :operation_type >= 3 AND :operation_type <= 8
    INSERT INTO redis_hash_test VALUES ('field_' || :client_id, 'value_' || :operation_type);
    SELECT * FROM redis_hash_test WHERE field LIKE 'field_%';
\endif

-- List operations (25% of operations)
\if :operation_type >= 9 AND :operation_type <= 12
    INSERT INTO redis_list_test VALUES ('item_' || :client_id || '_' || :operation_type);
    SELECT * FROM redis_list_test;
\endif

-- Set operations (15% of operations)
\if :operation_type >= 13 AND :operation_type <= 14
    INSERT INTO redis_set_test VALUES ('member_' || :client_id);
    SELECT * FROM redis_set_test;
\endif

-- Sorted set operations (5% of operations)
\if :operation_type = 15
    INSERT INTO redis_zset_test VALUES ('player_' || :client_id, :client_id::float8);
    SELECT * FROM redis_zset_test ORDER BY score DESC LIMIT 10;
\endif
EOF

    # Test script 2: Heavy INSERT operations
    cat > pgbench_redis_insert.sql << 'EOF'
-- Heavy INSERT operations test script
\set client_id random(1, 10000)
\set table_type random(1, 5)

\if :table_type = 1
    INSERT INTO redis_hash_test VALUES ('user_' || :client_id, 'data_' || :client_id);
\endif

\if :table_type = 2
    INSERT INTO redis_list_test VALUES ('task_' || :client_id);
\endif

\if :table_type = 3
    INSERT INTO redis_set_test VALUES ('tag_' || :client_id);
\endif

\if :table_type = 4
    INSERT INTO redis_zset_test VALUES ('score_' || :client_id, (:client_id % 1000)::float8);
\endif

\if :table_type = 5
    DELETE FROM redis_string_test;
    INSERT INTO redis_string_test VALUES ('config_' || :client_id);
\endif
EOF

    # Test script 3: Heavy SELECT operations
    cat > pgbench_redis_select.sql << 'EOF'
-- Heavy SELECT operations test script
\set table_type random(1, 5)

\if :table_type = 1
    SELECT COUNT(*) FROM redis_hash_test;
\endif

\if :table_type = 2
    SELECT COUNT(*) FROM redis_list_test;
\endif

\if :table_type = 3
    SELECT COUNT(*) FROM redis_set_test;
\endif

\if :table_type = 4
    SELECT COUNT(*) FROM redis_zset_test;
\endif

\if :table_type = 5
    SELECT * FROM redis_string_test;
\endif
EOF

    # Test script 4: DELETE operations
    cat > pgbench_redis_delete.sql << 'EOF'
-- DELETE operations test script
\set client_id random(1, 1000)
\set table_type random(1, 4)

\if :table_type = 1
    DELETE FROM redis_hash_test WHERE field = 'user_' || :client_id;
\endif

\if :table_type = 2
    DELETE FROM redis_list_test WHERE element = 'task_' || :client_id;
\endif

\if :table_type = 3
    DELETE FROM redis_set_test WHERE member = 'tag_' || :client_id;
\endif

\if :table_type = 4
    DELETE FROM redis_zset_test WHERE member = 'score_' || :client_id;
\endif
EOF

    echo "✅ pgbench test scripts created"
}

# Function to run initial data setup
setup_initial_data() {
    echo "Setting up initial test data..."
    
    # Clear existing data
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" FLUSHDB
    
    # Insert some initial data for testing
    execute_sql "INSERT INTO redis_hash_test VALUES ('initial_user_1', 'John Doe'), ('initial_user_2', 'Jane Smith');"
    execute_sql "INSERT INTO redis_list_test VALUES ('initial_task_1'), ('initial_task_2'), ('initial_task_3');"
    execute_sql "INSERT INTO redis_set_test VALUES ('initial_tag_1'), ('initial_tag_2');"
    execute_sql "INSERT INTO redis_zset_test VALUES ('initial_player_1', 100.0), ('initial_player_2', 95.5);"
    execute_sql "INSERT INTO redis_string_test VALUES ('initial_config_value');"
    
    echo "✅ Initial test data setup complete"
}

# Function to run pgbench tests
run_pgbench_tests() {
    echo ""
    echo "🏁 Running pgbench load tests..."
    echo "================================="
    
    # Test 1: Mixed operations
    echo ""
    echo "Test 1: Mixed operations load test"
    echo "-----------------------------------"
    pgbench -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" \
        -c "$CLIENTS" -j "$JOBS" -T "$DURATION" -P 5 \
        -f pgbench_redis_mixed.sql \
        --log --log-prefix="redis_mixed"
    
    # Test 2: Heavy INSERT operations
    echo ""
    echo "Test 2: Heavy INSERT operations load test"
    echo "------------------------------------------"
    pgbench -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" \
        -c "$CLIENTS" -j "$JOBS" -T "$DURATION" -P 5 \
        -f pgbench_redis_insert.sql \
        --log --log-prefix="redis_insert"
    
    # Test 3: Heavy SELECT operations
    echo ""
    echo "Test 3: Heavy SELECT operations load test"
    echo "------------------------------------------"
    pgbench -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" \
        -c "$CLIENTS" -j "$JOBS" -T "$DURATION" -P 5 \
        -f pgbench_redis_select.sql \
        --log --log-prefix="redis_select"
    
    # Test 4: DELETE operations
    echo ""
    echo "Test 4: DELETE operations load test"
    echo "------------------------------------"
    pgbench -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" \
        -c "$CLIENTS" -j "$JOBS" -T 30 -P 5 \
        -f pgbench_redis_delete.sql \
        --log --log-prefix="redis_delete"
}

# Function to analyze results
analyze_results() {
    echo ""
    echo "📊 Test Results Analysis"
    echo "========================"
    
    # Count final data in Redis
    echo "Final Redis data counts:"
    echo "- Hash entries: $(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" HLEN test:hash)"
    echo "- List entries: $(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" LLEN test:list)"
    echo "- Set entries: $(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" SCARD test:set)"
    echo "- ZSet entries: $(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ZCARD test:zset)"
    echo "- String value: $(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" GET test:string)"
    
    # Show PostgreSQL table counts
    echo ""
    echo "PostgreSQL foreign table counts:"
    execute_sql "SELECT 'Hash' as table_type, COUNT(*) as count FROM redis_hash_test UNION ALL SELECT 'List' as table_type, COUNT(*) as count FROM redis_list_test UNION ALL SELECT 'Set' as table_type, COUNT(*) as count FROM redis_set_test UNION ALL SELECT 'ZSet' as table_type, COUNT(*) as count FROM redis_zset_test UNION ALL SELECT 'String' as table_type, COUNT(*) as count FROM redis_string_test;"
    
    # Show log files created
    echo ""
    echo "Performance log files created:"
    ls -la pgbench_log.redis_*.* 2>/dev/null || echo "No log files found"
    
    echo ""
    echo "🎉 Load testing complete!"
    echo "Check the log files for detailed performance metrics."
}

# Function to cleanup
cleanup() {
    echo ""
    echo "🧹 Cleaning up..."
    rm -f pgbench_redis_*.sql
    echo "✅ Cleanup complete"
}

# Main execution
main() {
    check_redis
    setup_fdw
    create_foreign_tables
    create_pgbench_scripts
    setup_initial_data
    run_pgbench_tests
    analyze_results
    cleanup
}

# Handle script interruption
trap cleanup EXIT

# Run the main function
main "$@"
