#!/bin/bash
# Redis FDW Unified Load Testing & Benchmark Script
# Combines setup, data population, and pgbench benchmarking into a single tool
#
# Usage: ./redis_fdw_benchmark.sh {setup|populate|bench|stress|full|cleanup|help}

set -e

# ============================================================================
# Configuration - Customize these for your environment
# ============================================================================
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-8899}"
PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-28814}"
PG_USER="${PG_USER:-azureuser}"
PG_DATABASE="${PG_DATABASE:-postgres}"

# Benchmark parameters
DURATION="${DURATION:-30}"              # Test duration in seconds
WARMUP_DURATION="${WARMUP_DURATION:-5}" # Warmup duration
CLIENT_COUNTS="${CLIENT_COUNTS:-1 2 4 8 16 32}"  # Space-separated client counts
DATA_SIZE="${DATA_SIZE:-1000}"          # Number of test keys to create

# Pool configuration for foreign server
POOL_MAX_SIZE="${POOL_MAX_SIZE:-128}"
POOL_MIN_IDLE="${POOL_MIN_IDLE:-16}"
POOL_CONN_TIMEOUT_MS="${POOL_CONN_TIMEOUT_MS:-10000}"

# Output directory
RESULTS_DIR="${RESULTS_DIR:-./benchmark_results_$(date +%Y%m%d_%H%M%S)}"
LOG_FILE=""

# ============================================================================
# Utility Functions
# ============================================================================
log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "$msg"
    if [ -n "$LOG_FILE" ]; then
        echo "$msg" >> "$LOG_FILE"
    fi
}

log_error() {
    log "❌ ERROR: $*" >&2
}

log_success() {
    log "✅ $*"
}

log_info() {
    log "ℹ️  $*"
}

execute_sql() {
    echo "$1" | psql --no-psqlrc -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE"
}

execute_sql_file() {
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" -f "$1"
}

execute_sql_quiet() {
    echo "$1" | psql --no-psqlrc -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" -t -A 2>/dev/null
}

redis_cmd() {
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" "$@"
}

check_dependencies() {
    local missing=()
    
    command -v psql >/dev/null 2>&1 || missing+=("psql")
    command -v pgbench >/dev/null 2>&1 || missing+=("pgbench")
    command -v redis-cli >/dev/null 2>&1 || missing+=("redis-cli")
    
    if [ ${#missing[@]} -gt 0 ]; then
        log_error "Missing required tools: ${missing[*]}"
        exit 1
    fi
}

# ============================================================================
# Setup Functions
# ============================================================================
check_redis() {
    log_info "Checking Redis server at $REDIS_HOST:$REDIS_PORT..."
    
    if ! redis_cmd ping > /dev/null 2>&1; then
        log_error "Redis server is not running at $REDIS_HOST:$REDIS_PORT"
        echo ""
        echo "To start Redis with Docker:"
        echo "  docker run -d --name redis-bench -p $REDIS_PORT:6379 redis"
        echo ""
        echo "Or start Redis manually:"
        echo "  redis-server --port $REDIS_PORT"
        return 1
    fi
    
    log_success "Redis server is running"
    return 0
}

check_postgres() {
    log_info "Checking PostgreSQL at $PG_HOST:$PG_PORT..."
    
    if ! echo "SELECT 1" | psql --no-psqlrc -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" > /dev/null 2>&1; then
        log_error "Cannot connect to PostgreSQL at $PG_HOST:$PG_PORT"
        return 1
    fi
    
    log_success "PostgreSQL is accessible"
    return 0
}

check_foreign_tables() {
    # Check if the foreign tables already exist
    local table_exists
    table_exists=$(execute_sql_quiet "SELECT 1 FROM information_schema.tables WHERE table_name = 'redis_hash_bench' LIMIT 1;")
    
    if [ "$table_exists" = "1" ]; then
        return 0
    else
        return 1
    fi
}

check_redis_data() {
    # Check if Redis has test data
    local key_count
    key_count=$(redis_cmd EVAL "return #redis.call('KEYS', 'bench:0')" 0 2>/dev/null || echo "0")
    
    if [ "$key_count" -gt 0 ] 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

setup_extension() {
    log_info "Setting up Redis FDW extension..."
    
    # Create extension (ignore errors if already exists)
    psql --no-psqlrc -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" \
        -c "CREATE EXTENSION IF NOT EXISTS redis_fdw_rs;" > /dev/null 2>&1 || true
    
    # Verify extension exists
    local ext_exists
    ext_exists=$(psql --no-psqlrc -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" \
        -t -A -c "SELECT 1 FROM pg_extension WHERE extname = 'redis_fdw_rs';" 2>/dev/null || echo "")
    
    if [ "$ext_exists" = "1" ]; then
        log_success "Redis FDW extension is ready"
        return 0
    else
        log_error "Failed to create Redis FDW extension"
        echo "Make sure the extension is installed: cargo pgrx install --release"
        return 1
    fi
}

setup_foreign_tables() {
    log_info "Creating foreign server and tables..."
    
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" << EOF
-- Drop existing objects
DROP FOREIGN TABLE IF EXISTS redis_hash_bench CASCADE;
DROP FOREIGN TABLE IF EXISTS redis_string_bench CASCADE;
DROP FOREIGN TABLE IF EXISTS redis_list_bench CASCADE;
DROP FOREIGN TABLE IF EXISTS redis_set_bench CASCADE;
DROP FOREIGN TABLE IF EXISTS redis_zset_bench CASCADE;
DROP SERVER IF EXISTS redis_bench_server CASCADE;

-- Create foreign server with pool settings
CREATE SERVER redis_bench_server
    FOREIGN DATA WRAPPER redis_wrapper
    OPTIONS (
        host_port '${REDIS_HOST}:${REDIS_PORT}',
        database '0',
        pool_max_size '${POOL_MAX_SIZE}',
        pool_min_idle '${POOL_MIN_IDLE}',
        pool_connection_timeout_ms '${POOL_CONN_TIMEOUT_MS}'
    );

-- Create user mapping
CREATE USER MAPPING FOR CURRENT_USER 
    SERVER redis_bench_server 
    OPTIONS (user 'default');

-- Create foreign tables
CREATE FOREIGN TABLE redis_hash_bench (
    key TEXT,
    field TEXT,
    value TEXT
) SERVER redis_bench_server
OPTIONS (table_type 'hash', table_key_prefix 'bench:');

CREATE FOREIGN TABLE redis_string_bench (
    key TEXT,
    value TEXT
) SERVER redis_bench_server
OPTIONS (table_type 'string', table_key_prefix 'str:');

CREATE FOREIGN TABLE redis_list_bench (
    key TEXT,
    index INT,
    value TEXT
) SERVER redis_bench_server
OPTIONS (table_type 'list', table_key_prefix 'list:');

CREATE FOREIGN TABLE redis_set_bench (
    key TEXT,
    member TEXT
) SERVER redis_bench_server
OPTIONS (table_type 'set', table_key_prefix 'set:');

CREATE FOREIGN TABLE redis_zset_bench (
    key TEXT,
    member TEXT,
    score FLOAT
) SERVER redis_bench_server
OPTIONS (table_type 'zset', table_key_prefix 'zset:');
EOF

    log_success "Foreign tables created"
}

# ============================================================================
# Data Population
# ============================================================================
populate_redis_data() {
    log_info "Populating Redis with $DATA_SIZE test keys per type..."
    
    local hash_count=$((DATA_SIZE))
    local string_count=$((DATA_SIZE))
    local list_count=$((DATA_SIZE / 10))
    local set_count=$((DATA_SIZE / 10))
    local zset_count=$((DATA_SIZE / 10))
    
    redis_cmd EVAL "for i=0,$((hash_count-1)) do redis.call('HSET', 'bench:' .. i, 'field1', 'value' .. i, 'field2', 'data' .. i, 'field3', 'extra' .. i) end return $hash_count" 0
    log_info "Created $hash_count hash keys"
    
    redis_cmd EVAL "for i=0,$((string_count-1)) do redis.call('SET', 'str:' .. i, 'string_value_' .. i) end return $string_count" 0
    log_info "Created $string_count string keys"
    
    redis_cmd EVAL "for i=0,$((list_count-1)) do for j=0,9 do redis.call('RPUSH', 'list:' .. i, 'item_' .. j) end end return $list_count" 0
    log_info "Created $list_count list keys"
    
    redis_cmd EVAL "for i=0,$((set_count-1)) do for j=0,9 do redis.call('SADD', 'set:' .. i, 'member_' .. j) end end return $set_count" 0
    log_info "Created $set_count set keys"
    
    redis_cmd EVAL "for i=0,$((zset_count-1)) do for j=0,9 do redis.call('ZADD', 'zset:' .. i, j * 10, 'member_' .. j) end end return $zset_count" 0
    log_info "Created $zset_count zset keys"
    
    log_success "Redis test data populated"
    redis_cmd INFO keyspace | grep -E "^db|keys"
}

# ============================================================================
# Benchmark Functions
# ============================================================================
init_results_dir() {
    mkdir -p "$RESULTS_DIR"
    LOG_FILE="$RESULTS_DIR/benchmark.log"
    log_info "Results will be saved to: $RESULTS_DIR"
}

create_sql_files() {
    log_info "Creating pgbench SQL files..."
    
    # Hash SELECT test
    cat > "$RESULTS_DIR/hash_select.sql" << 'EOF'
SELECT * FROM redis_hash_bench WHERE key = 'bench:' || (random() * 999)::int;
EOF

    # Hash INSERT test
    cat > "$RESULTS_DIR/hash_insert.sql" << 'EOF'
INSERT INTO redis_hash_bench (key, field, value) 
VALUES ('bench:new:' || (random() * 10000)::int, 'field1', 'value_' || (random() * 10000)::int);
EOF

    # String SELECT test
    cat > "$RESULTS_DIR/string_select.sql" << 'EOF'
SELECT * FROM redis_string_bench WHERE key = 'str:' || (random() * 999)::int;
EOF

    # List SELECT test
    cat > "$RESULTS_DIR/list_select.sql" << 'EOF'
SELECT * FROM redis_list_bench WHERE key = 'list:' || (random() * 99)::int LIMIT 10;
EOF

    # Set SELECT test
    cat > "$RESULTS_DIR/set_select.sql" << 'EOF'
SELECT * FROM redis_set_bench WHERE key = 'set:' || (random() * 99)::int;
EOF

    # ZSet SELECT test
    cat > "$RESULTS_DIR/zset_select.sql" << 'EOF'
SELECT * FROM redis_zset_bench WHERE key = 'zset:' || (random() * 99)::int;
EOF

    # Mixed workload: reads and writes
    cat > "$RESULTS_DIR/mixed.sql" << 'EOF'
SELECT * FROM redis_hash_bench WHERE key = 'bench:' || (random() * 999)::int;
SELECT * FROM redis_string_bench WHERE key = 'str:' || (random() * 999)::int;
INSERT INTO redis_hash_bench (key, field, value) VALUES ('bench:mix:' || (random() * 10000)::int, 'f1', 'v1');
EOF

    log_success "SQL files created"
}

run_single_benchmark() {
    local test_name="$1"
    local sql_file="$2"
    local clients="$3"
    local duration="$4"
    
    log "Running: $test_name | Clients: $clients | Duration: ${duration}s"
    
    local output_file="$RESULTS_DIR/${test_name}_c${clients}.txt"
    
    pgbench -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" \
        -c "$clients" \
        -j "$clients" \
        -T "$duration" \
        -f "$sql_file" \
        -P 5 \
        --no-vacuum \
        2>&1 | tee "$output_file"
    
    # Extract metrics
    local tps=$(grep -E "^tps = |tps:" "$output_file" | tail -1 | grep -oE '[0-9]+\.[0-9]+' | head -1)
    local latency=$(grep -i "latency average" "$output_file" | grep -oE '[0-9]+\.[0-9]+' | head -1)
    
    tps=${tps:-0}
    latency=${latency:-0}
    
    log "  Result: TPS=$tps, Latency=${latency}ms"
    echo "$test_name,$clients,$tps,$latency" >> "$RESULTS_DIR/results.csv"
}

run_warmup() {
    log_info "Warming up connection pool for ${WARMUP_DURATION}s..."
    
    pgbench -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" \
        -c 4 -j 4 -T "$WARMUP_DURATION" \
        -f "$RESULTS_DIR/hash_select.sql" \
        --no-vacuum > /dev/null 2>&1 || true
    
    log_success "Warmup complete"
}

run_benchmark_suite() {
    log "=============================================="
    log "Starting Benchmark Suite"
    log "=============================================="
    
    # Initialize CSV
    echo "test_name,clients,tps,latency_ms" > "$RESULTS_DIR/results.csv"
    
    run_warmup
    
    # Convert CLIENT_COUNTS to array
    read -ra clients_array <<< "$CLIENT_COUNTS"
    
    local tests=("hash_select" "string_select" "list_select" "set_select" "zset_select" "mixed")
    
    for clients in "${clients_array[@]}"; do
        log ""
        log "=== Testing with $clients concurrent clients ==="
        
        for test in "${tests[@]}"; do
            local sql_file="$RESULTS_DIR/${test}.sql"
            if [ -f "$sql_file" ]; then
                run_single_benchmark "$test" "$sql_file" "$clients" "$DURATION"
                sleep 1
            fi
        done
    done
    
    log ""
    log_success "Benchmark suite completed"
}

run_quick_test() {
    log_info "Running quick sanity test..."
    
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" << EOF
SELECT 'Hash count:' as type, COUNT(*)::text as count FROM redis_hash_bench
UNION ALL
SELECT 'String count:', COUNT(*)::text FROM redis_string_bench
UNION ALL
SELECT 'List count:', COUNT(*)::text FROM redis_list_bench
UNION ALL
SELECT 'Set count:', COUNT(*)::text FROM redis_set_bench
UNION ALL
SELECT 'ZSet count:', COUNT(*)::text FROM redis_zset_bench;
EOF
    
    log_success "Quick test passed - foreign tables are working"
}

run_stress_test() {
    log "=============================================="
    log "Running Stress Test (Extended Duration)"
    log "=============================================="
    
    local stress_duration=60
    read -ra clients_array <<< "$CLIENT_COUNTS"
    
    for clients in "${clients_array[@]}"; do
        log ""
        log "Stress test: $clients clients for ${stress_duration}s"
        
        pgbench -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DATABASE" \
            -c "$clients" \
            -j "$clients" \
            -T "$stress_duration" \
            -f "$RESULTS_DIR/mixed.sql" \
            -P 10 \
            --no-vacuum \
            2>&1 | tee "$RESULTS_DIR/stress_c${clients}.txt"
        
        sleep 5
    done
    
    log_success "Stress test completed"
}

# ============================================================================
# Report Generation
# ============================================================================
generate_report() {
    log_info "Generating benchmark report..."
    
    cat > "$RESULTS_DIR/report.md" << EOF
# Redis FDW Benchmark Report

**Generated:** $(date)

## Configuration

| Parameter | Value |
|-----------|-------|
| PostgreSQL | $PG_HOST:$PG_PORT |
| Redis | $REDIS_HOST:$REDIS_PORT |
| Database | $PG_DATABASE |
| Test Duration | ${DURATION}s |
| Data Size | $DATA_SIZE keys |
| Pool Max Size | $POOL_MAX_SIZE |
| Pool Min Idle | $POOL_MIN_IDLE |

## Results Summary

| Test | Clients | TPS | Latency (ms) |
|------|---------|-----|--------------|
EOF
    
    if [ -f "$RESULTS_DIR/results.csv" ]; then
        tail -n +2 "$RESULTS_DIR/results.csv" | while IFS=, read -r test_name clients tps latency; do
            printf "| %s | %s | %s | %s |\n" "$test_name" "$clients" "$tps" "$latency" >> "$RESULTS_DIR/report.md"
        done
    fi
    
    cat >> "$RESULTS_DIR/report.md" << 'EOF'

## Test Descriptions

- **hash_select**: Random key lookups in hash tables
- **string_select**: Random string key lookups
- **list_select**: List range queries with LIMIT
- **set_select**: Set member retrieval
- **zset_select**: Sorted set queries
- **mixed**: Combined read/write workload

## Notes

The connection pool is shared globally across all queries, eliminating:
- Per-query connection pool creation overhead
- Repeated TCP handshakes and Redis AUTH
- Pool warmup latency

EOF
    
    log_success "Report saved to: $RESULTS_DIR/report.md"
    
    # Print summary to console
    echo ""
    echo "=============================================="
    echo "RESULTS SUMMARY"
    echo "=============================================="
    if [ -f "$RESULTS_DIR/results.csv" ]; then
        column -t -s',' "$RESULTS_DIR/results.csv"
    fi
}

# ============================================================================
# Cleanup
# ============================================================================
cleanup_redis_data() {
    log_info "Cleaning up Redis test data..."
    
    local prefixes=("bench:*" "str:*" "list:*" "set:*" "zset:*")
    
    for prefix in "${prefixes[@]}"; do
        local count=$(redis_cmd EVAL "local keys = redis.call('KEYS', '$prefix'); for i,k in ipairs(keys) do redis.call('DEL', k) end; return #keys" 0)
        log_info "Deleted $count keys matching $prefix"
    done
    
    log_success "Redis test data cleaned up"
}

cleanup_postgres() {
    log_info "Cleaning up PostgreSQL objects..."
    
    execute_sql "DROP FOREIGN TABLE IF EXISTS redis_hash_bench, redis_string_bench, redis_list_bench, redis_set_bench, redis_zset_bench CASCADE;" || true
    execute_sql "DROP SERVER IF EXISTS redis_bench_server CASCADE;" || true
    
    log_success "PostgreSQL objects cleaned up"
}

# ============================================================================
# Help
# ============================================================================
show_help() {
    cat << EOF
Redis FDW Unified Benchmark Script
===================================

Usage: $0 <command> [options]

Commands:
  setup     - Check connections, create extension and foreign tables
  populate  - Populate Redis with test data
  quick     - Run quick sanity test
  bench     - Run full benchmark suite
  stress    - Run extended stress test
  full      - Complete: setup + populate + quick + bench + report
  cleanup   - Remove test data from Redis and PostgreSQL
  help      - Show this help message

Environment Variables:
  REDIS_HOST    Redis hostname (default: 127.0.0.1)
  REDIS_PORT    Redis port (default: 8899)
  PG_HOST       PostgreSQL hostname (default: 127.0.0.1)
  PG_PORT       PostgreSQL port (default: 28814)
  PG_USER       PostgreSQL user (default: azureuser)
  PG_DATABASE   PostgreSQL database (default: postgres)
  
  DURATION      Benchmark duration in seconds (default: 30)
  CLIENT_COUNTS Space-separated list of client counts (default: "1 2 4 8 16 32")
  DATA_SIZE     Number of test keys per type (default: 1000)
  RESULTS_DIR   Output directory for results

Examples:
  # Full benchmark with defaults
  $0 full
  
  # Quick setup and test
  $0 setup && $0 populate && $0 quick
  
  # Custom benchmark
  DURATION=60 CLIENT_COUNTS="4 8 16" $0 bench
  
  # Different Redis/PostgreSQL servers
  REDIS_PORT=6379 PG_PORT=5432 $0 full

EOF
}

# ============================================================================
# Main
# ============================================================================
main() {
    local command="${1:-help}"
    
    echo "=============================================="
    echo "Redis FDW Benchmark Tool"
    echo "=============================================="
    echo "Redis:      $REDIS_HOST:$REDIS_PORT"
    echo "PostgreSQL: $PG_HOST:$PG_PORT ($PG_DATABASE)"
    echo "=============================================="
    echo ""
    
    check_dependencies
    
    case "$command" in
        setup)
            check_redis || exit 1
            check_postgres || exit 1
            setup_extension
            setup_foreign_tables
            ;;
        populate)
            check_redis || exit 1
            populate_redis_data
            ;;
        bench)
            check_redis || exit 1
            check_postgres || exit 1
            # Check if foreign tables exist, if not, set them up
            if ! check_foreign_tables; then
                log_info "Foreign tables not found, running setup first..."
                setup_extension
                setup_foreign_tables
            fi
            # Check if Redis test data exists, if not, populate
            if ! check_redis_data; then
                log_info "Redis test data not found, populating..."
                populate_redis_data
            fi
            init_results_dir
            create_sql_files
            run_benchmark_suite
            generate_report
            ;;
        stress)
            check_redis || exit 1
            check_postgres || exit 1
            # Check if foreign tables exist, if not, set them up
            if ! check_foreign_tables; then
                log_info "Foreign tables not found, running setup first..."
                setup_extension
                setup_foreign_tables
            fi
            # Check if Redis test data exists, if not, populate
            if ! check_redis_data; then
                log_info "Redis test data not found, populating..."
                populate_redis_data
            fi
            init_results_dir
            create_sql_files
            run_stress_test
            ;;
        full)
            check_redis || exit 1
            check_postgres || exit 1
            setup_extension
            setup_foreign_tables
            populate_redis_data
            run_quick_test
            init_results_dir
            create_sql_files
            run_benchmark_suite
            generate_report
            ;;
        cleanup)
            cleanup_redis_data
            cleanup_postgres
            ;;
        help|--help|-h)
            show_help
            exit 0
            ;;
        *)
            log_error "Unknown command: $command"
            echo ""
            show_help
            exit 1
            ;;
    esac
    
    echo ""
    log_success "Command '$command' completed successfully"
    if [ -n "$RESULTS_DIR" ] && [ -d "$RESULTS_DIR" ]; then
        echo "Results: $RESULTS_DIR"
    fi
}

main "$@"
