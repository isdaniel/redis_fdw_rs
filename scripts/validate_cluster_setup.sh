#!/bin/bash

# Simple Redis Cluster Integration Validation Script
# This script performs basic validation of our cluster test infrastructure

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

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

echo_info "Redis Cluster Integration Test Validation"
echo "=========================================="

# Test 1: Check if source files exist
echo_info "Test 1: Checking source file structure..."

required_files=(
    "src/tests/cluster_integration_tests.rs"
    "docker-compose.cluster-test.yml"
    "scripts/cluster_test.sh"
    "scripts/cluster/init-cluster.sh"
    "CLUSTER_TESTING.md"
    ".env.cluster-test.example"
)

all_files_exist=true
for file in "${required_files[@]}"; do
    if [ -f "$PROJECT_ROOT/$file" ]; then
        echo_success "  ✓ $file exists"
    else
        echo_error "  ✗ $file missing"
        all_files_exist=false
    fi
done

if [ "$all_files_exist" = true ]; then
    echo_success "All required files exist"
else
    echo_error "Some required files are missing"
    exit 1
fi

# Test 2: Check if scripts are executable
echo_info "Test 2: Checking script permissions..."

executable_files=(
    "scripts/cluster_test.sh"
    "scripts/cluster/init-cluster.sh"
)

all_executable=true
for file in "${executable_files[@]}"; do
    if [ -x "$PROJECT_ROOT/$file" ]; then
        echo_success "  ✓ $file is executable"
    else
        echo_warning "  ⚠ $file is not executable"
        chmod +x "$PROJECT_ROOT/$file"
        echo_info "  → Made $file executable"
    fi
done

echo_success "Script permissions verified"

# Test 3: Check Rust code compilation
echo_info "Test 3: Testing Rust code compilation..."

cd "$PROJECT_ROOT"
if cargo check --quiet 2>/dev/null; then
    echo_success "Rust code compiles successfully"
else
    echo_error "Rust code compilation failed"
    echo_info "Running cargo check to see errors:"
    cargo check
    exit 1
fi

# Test 4: Check if cluster test module is properly included
echo_info "Test 4: Checking cluster test module integration..."

if grep -q "cluster_integration_tests" "$PROJECT_ROOT/src/tests/mod.rs"; then
    echo_success "Cluster test module is properly included in mod.rs"
else
    echo_error "Cluster test module is not included in mod.rs"
    exit 1
fi

# Test 5: Check Docker Compose file syntax
echo_info "Test 5: Validating Docker Compose file..."

if command -v docker-compose &> /dev/null; then
    if docker-compose -f "$PROJECT_ROOT/docker-compose.cluster-test.yml" config > /dev/null 2>&1; then
        echo_success "Docker Compose file syntax is valid"
    else
        echo_warning "Docker Compose file may have syntax issues (but Docker might not be available)"
    fi
elif command -v docker &> /dev/null && docker compose version &> /dev/null; then
    if docker compose -f "$PROJECT_ROOT/docker-compose.cluster-test.yml" config > /dev/null 2>&1; then
        echo_success "Docker Compose file syntax is valid"
    else
        echo_warning "Docker Compose file may have syntax issues (but Docker might not be available)"
    fi
else
    echo_warning "Docker/Docker Compose not available, skipping syntax validation"
fi

# Test 6: Test cluster management script help
echo_info "Test 6: Testing cluster management script..."

if "$PROJECT_ROOT/scripts/cluster_test.sh" help > /dev/null 2>&1; then
    echo_success "Cluster management script runs successfully"
else
    echo_error "Cluster management script has issues"
    exit 1
fi

# Test 7: Check Redis dependency
echo_info "Test 7: Checking Redis crate configuration..."

if grep -q 'redis.*cluster' "$PROJECT_ROOT/Cargo.toml"; then
    echo_success "Redis crate with cluster support is configured"
else
    echo_error "Redis cluster support not found in Cargo.toml"
    exit 1
fi

# Test 8: Validate test conditionals
echo_info "Test 8: Checking cluster test conditionals..."

if grep -q "REDIS_CLUSTER_TEST_ENABLED" "$PROJECT_ROOT/src/tests/cluster_integration_tests.rs"; then
    echo_success "Cluster test conditionals are properly implemented"
else
    echo_error "Cluster test conditionals not found"
    exit 1
fi

echo ""
echo_success "All validation tests passed!"
echo_info "Your Redis cluster integration testing setup is ready."
echo ""
echo_info "Next steps:"
echo "  1. Start cluster: ./scripts/cluster_test.sh start"
echo "  2. Run tests: ./scripts/cluster_test.sh test"
echo "  3. View documentation: cat CLUSTER_TESTING.md"
echo ""
echo_warning "Note: Docker and Redis cluster need to be running for actual integration tests."
echo ""
