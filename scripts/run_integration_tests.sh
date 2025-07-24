#!/bin/bash

# Redis FDW Integration Test Runner
# This script helps set up and run integration tests for the Redis FDW

set -e

REDIS_PORT=8899
REDIS_PID_FILE="/tmp/redis_test_${REDIS_PORT}.pid"
REDIS_CONFIG_FILE="/tmp/redis_test_${REDIS_PORT}.conf"
REDIS_LOG_FILE="/tmp/redis_test_${REDIS_PORT}.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

echo_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

echo_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

echo_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Redis is available
check_redis_available() {
    if ! command -v redis-server &> /dev/null; then
        echo_error "redis-server is not installed or not in PATH"
        echo "Please install Redis server:"
        echo "  Ubuntu/Debian: sudo apt-get install redis-server"
        echo "  CentOS/RHEL: sudo yum install redis"
        echo "  macOS: brew install redis"
        exit 1
    fi
}

# Function to start Redis test server
start_redis() {
    echo_info "Starting Redis test server on port ${REDIS_PORT}..."
    
    # Create Redis configuration for testing
    cat > "${REDIS_CONFIG_FILE}" << EOF
# Redis test configuration for FDW integration tests
port ${REDIS_PORT}
bind 127.0.0.1
timeout 0
tcp-keepalive 300
daemonize yes
pidfile ${REDIS_PID_FILE}
loglevel notice
logfile ${REDIS_LOG_FILE}
databases 16
save ""
# Disable RDB persistence for tests
rdbcompression no
dbfilename ""
dir /tmp
appendonly no
# Security settings for test environment
protected-mode no
# Memory management for tests  
maxmemory 128mb
maxmemory-policy allkeys-lru
EOF

    # Start Redis server
    redis-server "${REDIS_CONFIG_FILE}"
    
    # Wait for Redis to start
    sleep 2
    
    # Check if Redis is running
    if ! redis-cli -p "${REDIS_PORT}" ping &> /dev/null; then
        echo_error "Failed to start Redis server on port ${REDIS_PORT}"
        exit 1
    fi
    
    echo_success "Redis test server started on port ${REDIS_PORT}"
}

# Function to stop Redis test server
stop_redis() {
    echo_info "Stopping Redis test server..."
    
    if [ -f "${REDIS_PID_FILE}" ]; then
        local redis_pid=$(cat "${REDIS_PID_FILE}")
        if kill -0 "$redis_pid" 2>/dev/null; then
            kill "$redis_pid"
            sleep 1
            
            # Force kill if still running
            if kill -0 "$redis_pid" 2>/dev/null; then
                kill -9 "$redis_pid"
            fi
        fi
        rm -f "${REDIS_PID_FILE}"
    fi
    
    # Clean up configuration and log files
    rm -f "${REDIS_CONFIG_FILE}" "${REDIS_LOG_FILE}"
    
    echo_success "Redis test server stopped"
}

# Function to clean Redis test data
clean_redis() {
    echo_info "Cleaning Redis test data..."
    
    if redis-cli -p "${REDIS_PORT}" ping &> /dev/null; then
        # Clean all databases
        redis-cli -p "${REDIS_PORT}" FLUSHALL
        echo_success "Redis test data cleaned"
    else
        echo_warning "Redis server not running, skipping data cleanup"
    fi
}

# Function to run integration tests
run_tests() {
    local pg_version=${1:-"pg14"}
    local test_pattern=${2:-""}
    
    echo_info "Running Redis FDW integration tests for ${pg_version}..."
    
    if [ -n "$test_pattern" ]; then
        echo_info "Running tests matching pattern: ${test_pattern}"
        cargo pgrx test "${pg_version}" "${test_pattern}"
    else
        echo_info "Running all integration tests"
        cargo pgrx test "${pg_version}"
    fi
}

# Function to run specific test modules
run_test_module() {
    local pg_version=${1:-"pg14"}
    local module=$2
    
    case $module in
        "hash")
            echo_info "Running Hash table integration tests..."
            run_tests "$pg_version" "test_hash_table"
            ;;
        "list")
            echo_info "Running List table integration tests..."
            run_tests "$pg_version" "test_list_table"
            ;;
        "set")
            echo_info "Running Set table integration tests..."
            run_tests "$pg_version" "test_set_table"
            ;;
        "string")
            echo_info "Running String table integration tests..."
            run_tests "$pg_version" "test_string_table"
            ;;
        "zset")
            echo_info "Running ZSet table integration tests..."
            run_tests "$pg_version" "test_zset_table"
            ;;
        "cross")
            echo_info "Running Cross-table integration tests..."
            run_tests "$pg_version" "cross_table"
            ;;
        "error")
            echo_info "Running Error handling tests..."
            run_tests "$pg_version" "error_handling"
            ;;
        "performance")
            echo_info "Running Performance tests..."
            run_tests "$pg_version" "performance"
            ;;
        *)
            echo_error "Unknown test module: $module"
            echo "Available modules: hash, list, set, string, zset, cross, error, performance"
            exit 1
            ;;
    esac
}

# Function to show usage
show_usage() {
    cat << EOF
Redis FDW Integration Test Runner

Usage: $0 [COMMAND] [OPTIONS]

Commands:
    setup       Start Redis test server
    cleanup     Stop Redis test server and clean up
    clean-data  Clean Redis test data (keep server running)
    test        Run integration tests
    test-module Run specific test module
    status      Check Redis server status
    help        Show this help message

Test Options:
    -p, --pg-version    PostgreSQL version (pg14, pg15, pg16, pg17) [default: pg14]
    -m, --module        Test module (hash, list, set, string, zset, cross, error, performance)
    -t, --test-pattern  Specific test pattern to run

Examples:
    $0 setup                           # Start Redis test server
    $0 test                            # Run all integration tests (pg14)
    $0 test -p pg17                    # Run all integration tests (pg17)
    $0 test-module -m hash             # Run only hash table tests
    $0 test -t test_hash_table_crud    # Run specific test
    $0 cleanup                         # Stop server and clean up

Requirements:
    - Redis server installed and available in PATH
    - PostgreSQL 14-17 configured with pgrx
    - Rust toolchain with cargo-pgrx installed

Redis Configuration:
    Host: 127.0.0.1
    Port: ${REDIS_PORT}
    Databases: 0-15 (test database: 15)
EOF
}

# Function to check Redis status
check_status() {
    echo_info "Checking Redis test server status..."
    
    if redis-cli -p "${REDIS_PORT}" ping &> /dev/null; then
        echo_success "Redis server is running on port ${REDIS_PORT}"
        
        # Show some basic info
        echo_info "Redis info:"
        redis-cli -p "${REDIS_PORT}" info server | grep -E "(redis_version|redis_mode|tcp_port|uptime_in_seconds)"
        
        echo_info "Database sizes:"
        for db in {0..15}; do
            size=$(redis-cli -p "${REDIS_PORT}" -n "$db" DBSIZE 2>/dev/null || echo "0")
            if [ "$size" != "0" ]; then
                echo "  DB${db}: ${size} keys"
            fi
        done
    else
        echo_error "Redis server is not running on port ${REDIS_PORT}"
        return 1
    fi
}

# Trap to ensure cleanup on script exit
trap 'if [ "$CLEANUP_ON_EXIT" = "true" ]; then stop_redis; fi' EXIT

# Main script logic
main() {
    local command=${1:-"help"}
    shift || true
    
    case $command in
        "setup")
            check_redis_available
            start_redis
            echo_info "Redis test server is ready for integration tests"
            echo_info "Run '$0 test' to start testing"
            ;;
        "cleanup")
            stop_redis
            ;;
        "clean-data")
            clean_redis
            ;;
        "test")
            check_redis_available
            
            # Parse options
            local pg_version="pg14"
            local test_pattern=""
            
            while [[ $# -gt 0 ]]; do
                case $1 in
                    -p|--pg-version)
                        pg_version="$2"
                        shift 2
                        ;;
                    -t|--test-pattern)
                        test_pattern="$2"
                        shift 2
                        ;;
                    *)
                        echo_error "Unknown option: $1"
                        show_usage
                        exit 1
                        ;;
                esac
            done
            
            # Check if Redis is running, start if needed
            if ! redis-cli -p "${REDIS_PORT}" ping &> /dev/null; then
                echo_warning "Redis not running, starting test server..."
                start_redis
                CLEANUP_ON_EXIT=true
            fi
            
            run_tests "$pg_version" "$test_pattern"
            ;;
        "test-module")
            check_redis_available
            
            # Parse options
            local pg_version="pg14"
            local module=""
            
            while [[ $# -gt 0 ]]; do
                case $1 in
                    -p|--pg-version)
                        pg_version="$2"
                        shift 2
                        ;;
                    -m|--module)
                        module="$2"
                        shift 2
                        ;;
                    *)
                        echo_error "Unknown option: $1"
                        show_usage
                        exit 1
                        ;;
                esac
            done
            
            if [ -z "$module" ]; then
                echo_error "Module is required for test-module command"
                show_usage
                exit 1
            fi
            
            # Check if Redis is running, start if needed
            if ! redis-cli -p "${REDIS_PORT}" ping &> /dev/null; then
                echo_warning "Redis not running, starting test server..."
                start_redis
                CLEANUP_ON_EXIT=true
            fi
            
            run_test_module "$pg_version" "$module"
            ;;
        "status")
            check_status
            ;;
        "help"|"--help"|"-h")
            show_usage
            ;;
        *)
            echo_error "Unknown command: $command"
            show_usage
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"
