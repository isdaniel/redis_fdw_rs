#!/bin/bash

# Redis Cluster Test Management Script
# This script helps manage the Redis cluster for integration testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.cluster-test.yml"

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

# Function to check if Docker and Docker Compose are available
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo_error "Docker is not installed or not in PATH"
        echo "Please install Docker:"
        echo "  Ubuntu/Debian: https://docs.docker.com/engine/install/ubuntu/"
        exit 1
    fi

    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        echo_error "Docker Compose is not installed or not in PATH"
        echo "Please install Docker Compose:"
        echo "  https://docs.docker.com/compose/install/"
        exit 1
    fi

    # Test Docker connectivity
    if ! docker info &> /dev/null; then
        echo_error "Cannot connect to Docker daemon"
        echo "Please ensure Docker is running and you have permission to access it"
        echo "Try: sudo usermod -aG docker \$USER && newgrp docker"
        exit 1
    fi
}

# Function to get docker-compose command
get_compose_cmd() {
    if command -v docker-compose &> /dev/null; then
        echo "docker-compose"
    else
        echo "docker compose"
    fi
}

# Function to start the Redis cluster
start_cluster() {
    echo_info "Starting Redis cluster for testing..."
    
    COMPOSE_CMD=$(get_compose_cmd)
    
    # Create logs directory
    mkdir -p "$PROJECT_ROOT/test-logs"
    
    # Start the cluster
    cd "$PROJECT_ROOT"
    $COMPOSE_CMD -f "$COMPOSE_FILE" up -d
    
    if [ $? -eq 0 ]; then
        echo_success "Redis cluster started successfully!"
        echo_info "Cluster nodes are available at:"
        echo "  127.0.0.1:7001, 127.0.0.1:7002, 127.0.0.1:7003"
        echo "  127.0.0.1:7004, 127.0.0.1:7005, 127.0.0.1:7006"
        echo ""
        echo_info "For integration tests, use connection string:"
        echo "  127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006"
    else
        echo_error "Failed to start Redis cluster"
        exit 1
    fi
}

# Function to stop the Redis cluster
stop_cluster() {
    echo_info "Stopping Redis cluster..."
    
    COMPOSE_CMD=$(get_compose_cmd)
    
    cd "$PROJECT_ROOT"
    $COMPOSE_CMD -f "$COMPOSE_FILE" down
    
    if [ $? -eq 0 ]; then
        echo_success "Redis cluster stopped successfully!"
    else
        echo_error "Failed to stop Redis cluster"
        exit 1
    fi
}

# Function to clean up everything
cleanup_cluster() {
    echo_info "Cleaning up Redis cluster and data..."
    
    COMPOSE_CMD=$(get_compose_cmd)
    
    cd "$PROJECT_ROOT"
    $COMPOSE_CMD -f "$COMPOSE_FILE" down -v --remove-orphans
    
    # Remove logs
    if [ -d "$PROJECT_ROOT/test-logs" ]; then
        rm -rf "$PROJECT_ROOT/test-logs"
        echo_info "Removed test logs directory"
    fi
    
    echo_success "Redis cluster cleanup completed!"
}

# Function to check cluster status
status_cluster() {
    echo_info "Checking Redis cluster status..."
    
    COMPOSE_CMD=$(get_compose_cmd)
    
    cd "$PROJECT_ROOT"
    $COMPOSE_CMD -f "$COMPOSE_FILE" ps
    
    echo ""
    echo_info "Testing cluster connectivity..."
    
    # Test connection to first node
    if redis-cli -h 127.0.0.1 -p 7001 ping > /dev/null 2>&1; then
        echo_success "Cluster is reachable"
        echo ""
        echo "Cluster information:"
        redis-cli -h 127.0.0.1 -p 7001 cluster info
        echo ""
        echo "Cluster nodes:"
        redis-cli -h 127.0.0.1 -p 7001 cluster nodes
    else
        echo_error "Cluster is not reachable"
        echo "Try starting the cluster with: $0 start"
    fi
}

# Function to show logs
logs_cluster() {
    COMPOSE_CMD=$(get_compose_cmd)
    
    cd "$PROJECT_ROOT"
    if [ -n "$1" ]; then
        # Show logs for specific service
        $COMPOSE_CMD -f "$COMPOSE_FILE" logs -f "$1"
    else
        # Show logs for all services
        $COMPOSE_CMD -f "$COMPOSE_FILE" logs -f
    fi
}

# Function to run integration tests
test_cluster() {
    echo_info "Running Redis cluster integration tests..."
    
    # First check if cluster is running
    if ! redis-cli -h 127.0.0.1 -p 7001 ping > /dev/null 2>&1; then
        echo_warning "Redis cluster is not running. Starting it now..."
        start_cluster
        echo_info "Waiting for cluster to be fully initialized..."
        sleep 10
    fi
    
    echo_info "Running pgrx tests with cluster configuration..."
    
    # Set environment variable for cluster tests
    export REDIS_CLUSTER_NODES="127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006"
    export REDIS_CLUSTER_TEST_ENABLED="true"
    
    # Run the specific cluster tests
    cd "$PROJECT_ROOT"
    cargo pgrx test
    
    if [ $? -eq 0 ]; then
        echo_success "All cluster integration tests passed!"
    else
        echo_error "Some cluster integration tests failed"
        exit 1
    fi
}

# Function to show help
show_help() {
    echo "Redis Cluster Test Management Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start     Start the Redis cluster"
    echo "  stop      Stop the Redis cluster"
    echo "  cleanup   Stop cluster and remove all data"
    echo "  status    Check cluster status and connectivity"
    echo "  logs      Show cluster logs (optional: service name)"
    echo "  test      Run integration tests against the cluster"
    echo "  help      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 start                    # Start the cluster"
    echo "  $0 logs redis-cluster-1     # Show logs for node 1"
    echo "  $0 test                     # Run integration tests"
    echo ""
}

# Main script logic
main() {
    check_docker
    
    case "${1:-}" in
        start)
            start_cluster
            ;;
        stop)
            stop_cluster
            ;;
        cleanup)
            cleanup_cluster
            ;;
        status)
            status_cluster
            ;;
        logs)
            logs_cluster "$2"
            ;;
        test)
            test_cluster
            ;;
        help|--help|-h)
            show_help
            ;;
        "")
            echo_error "No command specified"
            echo ""
            show_help
            exit 1
            ;;
        *)
            echo_error "Unknown command: $1"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

main "$@"
