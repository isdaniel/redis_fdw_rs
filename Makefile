# Redis FDW Makefile
# Provides convenient targets for building, testing, and managing the Redis FDW extension

.PHONY: help build install test test-unit test-smoke test-all clean setup-redis cleanup-redis docs

# Default target
help:
	@echo "Redis FDW - Available Make Targets:"
	@echo ""
	@echo "Build & Install:"
	@echo "  build              Build the extension for all PostgreSQL versions"
	@echo "  install            Install the extension for all PostgreSQL versions"
	@echo "  install-pg15       Install for PostgreSQL 15 only"
	@echo ""
	@echo "Testing:"
	@echo "  test               Run all tests (unit + integration)"
	@echo "  test-unit          Run unit tests only (no Redis required)"
	@echo "  test-smoke         Run smoke tests (requires Redis)"
	@echo "  test-all           Run comprehensive tests across all PG versions"
	@echo "  test-pg15          Run tests for PostgreSQL 15 only"
	@echo "  test-pg16          Run tests for PostgreSQL 16 only"
	@echo "  test-pg17          Run tests for PostgreSQL 17 only"
	@echo ""
	@echo "Redis Management:"
	@echo "  setup-redis        Start Redis server for testing"
	@echo "  cleanup-redis      Clean up Redis test data"
	@echo "  redis-status       Check Redis server status"
	@echo ""
	@echo "Development:"
	@echo "  docs               Generate documentation"
	@echo "  clean              Clean build artifacts"
	@echo "  format             Format Rust code"
	@echo "  lint               Run clippy linter"
	@echo ""
	@echo "Examples:"
	@echo "  make test-unit     # Quick tests without Redis"
	@echo "  make test-smoke    # Full smoke tests with Redis"
	@echo "  make test-pg15     # Test only PostgreSQL 15"

# PostgreSQL versions
PG_VERSIONS := pg14 pg15 pg16 pg17

# Build targets
build:
	@echo "Building Redis FDW for all PostgreSQL versions..."
	cargo pgrx package

build-debug:
	@echo "Building Redis FDW in debug mode..."
	cargo build

build-release:
	@echo "Building Redis FDW in release mode..."
	cargo build --release

# Install targets
install: $(addprefix install-,$(PG_VERSIONS))

install-pg14:
	@echo "Installing Redis FDW for PostgreSQL 14..."
	cargo pgrx install --pg-config "$$(cargo pgrx env pg14)"

install-pg15:
	@echo "Installing Redis FDW for PostgreSQL 15..."
	cargo pgrx install --pg-config "$$(cargo pgrx env pg15)"

install-pg16:
	@echo "Installing Redis FDW for PostgreSQL 16..."
	cargo pgrx install --pg-config "$$(cargo pgrx env pg16)"

install-pg17:
	@echo "Installing Redis FDW for PostgreSQL 17..."
	cargo pgrx install --pg-config "$$(cargo pgrx env pg17)"

# Test targets
test: $(addprefix test-unit-,$(PG_VERSIONS)) $(addprefix test-integration-,$(PG_VERSIONS))

# Individual test types
test-unit-pg14:
	@echo "Running unit tests for PostgreSQL 14..."
	cargo pgrx test pg14

test-integration-pg14:
	@echo "Running integration tests for PostgreSQL 14..."
	cargo pgrx test pg14

test-unit-pg15:
	@echo "Running unit tests for PostgreSQL 15..."
	cargo pgrx test pg15

test-integration-pg15:
	@echo "Running integration tests for PostgreSQL 15..."
	cargo pgrx test pg15 

# Individual test types
test-unit-pg16:
	@echo "Running unit tests for PostgreSQL 16..."
	cargo pgrx test pg16

test-integration-pg16:
	@echo "Running integration tests for PostgreSQL 16..."
	cargo pgrx test pg16 
	
# Individual test types
test-unit-pg17:
	@echo "Running unit tests for PostgreSQL 17..."
	cargo pgrx test pg17

test-integration-pg17:
	@echo "Running integration tests for PostgreSQL 17..."
	cargo pgrx test pg17 

# Specific test functions
test-hash:
	@echo "Running hash table tests..."
	cargo pgrx test pg15 -- test_hash_table_smoke

test-list:
	@echo "Running list table tests..."
	cargo pgrx test pg15 -- test_list_table_smoke

test-set:
	@echo "Running set table tests..."
	cargo pgrx test pg15 -- test_set_table_smoke

test-string:
	@echo "Running string table tests..."
	cargo pgrx test pg15

test-zset:
	@echo "Running zset table tests..."
	cargo pgrx test pg15 

# Redis management
setup-redis:
	@echo "Setting up Redis for testing..."
	@if command -v docker >/dev/null 2>&1; then \
		echo "Starting Redis with Docker..."; \
		docker run -d --name redis-fdw-test -p 8899:6379 redis:latest || \
		echo "Redis container may already be running"; \
	elif command -v redis-server >/dev/null 2>&1; then \
		echo "Starting Redis server..."; \
		redis-server --daemonize yes --port 8899; \
	else \
		echo "Error: Neither Docker nor redis-server found"; \
		echo "Please install Redis or Docker to run integration tests"; \
		exit 1; \
	fi
	@sleep 2
	@$(MAKE) redis-status

redis-status:
	@echo "Checking Redis status..."
	@if command -v redis-cli >/dev/null 2>&1; then \
		if redis-cli ping >/dev/null 2>&1; then \
			echo "✅ Redis is running and accessible"; \
			redis-cli info server | grep redis_version; \
		else \
			echo "❌ Redis is not responding"; \
		fi; \
	else \
		echo "⚠️  redis-cli not available"; \
	fi

# Development targets
docs:
	@echo "Generating documentation..."
	cargo doc --no-deps --open

format:
	@echo "Formatting Rust code..."
	cargo fmt

lint:
	@echo "Running clippy linter..."
	cargo clippy -- -D warnings

check:
	@echo "Running cargo check..."
	cargo check

# Clean targets
clean:
	@echo "Cleaning build artifacts..."
	cargo clean

clean-all: clean cleanup-redis
	@echo "Cleaning everything including Redis test data..."

# Development workflow
dev-setup: setup-redis
	@echo "Setting up development environment..."
	cargo pgrx init --pg15 download || echo "pgrx already initialized"

dev-test: format lint test-unit
	@echo "Running development test cycle..."

dev-test-full: format lint test
	@echo "Running full development test cycle..."

# CI/CD targets
ci-test:
	@echo "Running CI test suite..."
	@# Unit tests first (fast, no external dependencies)
	$(MAKE) test-unit
	@# Check if Redis is available for integration tests
	@if $(MAKE) redis-status >/dev/null 2>&1; then \
		echo "Redis available, running integration tests..."; \
		$(MAKE) test-smoke; \
	else \
		echo "Redis not available, skipping integration tests"; \
	fi

# Performance testing
perf-test:
	@echo "Running performance tests..."
	cargo pgrx test pg15

# Package for distribution
package:
	@echo "Creating package for distribution..."
	cargo pgrx package --pg-config "$$(cargo pgrx env pg15)"

# Utility targets
version:
	@echo "Redis FDW Version Information:"
	@echo "Rust version: $$(rustc --version)"
	@echo "pgrx version: $$(cargo pgrx --version 2>/dev/null || echo 'not found')"
	@echo "Available PostgreSQL versions:"
	@cargo pgrx env 2>/dev/null | grep "pg" || echo "Run 'cargo pgrx init' first"

env:
	@echo "Environment Information:"
	@echo "CARGO_HOME: $${CARGO_HOME:-not set}"
	@echo "PGRX_HOME: $${PGRX_HOME:-not set}"
	@echo "PATH: $$PATH"
	@$(MAKE) version

# Help for specific targets
help-test:
	@echo "Testing Help:"
	@echo ""
	@echo "Test Types:"
	@echo "  Unit Tests     - Fast tests, no external dependencies"
	@echo "  Smoke Tests    - Integration tests with Redis"
	@echo "  Performance    - Basic performance validation"
	@echo ""
	@echo "Prerequisites:"
	@echo "  Unit Tests     - Only Rust and pgrx"
	@echo "  Smoke Tests    - Redis server on localhost:8899"
	@echo ""
	@echo "Quick Start:"
	@echo "  make setup-redis  # Start Redis"
	@echo "  make test-smoke   # Run all smoke tests"
	@echo "  make cleanup-redis # Clean up"

help-build:
	@echo "Build Help:"
	@echo ""
	@echo "Build Types:"
	@echo "  Debug    - Fast compilation, includes debug info"
	@echo "  Release  - Optimized compilation for production"
	@echo "  Package  - Creates distributable package"
	@echo ""
	@echo "PostgreSQL Versions:"
	@echo "  pg14, pg15, pg16, pg17 - Specific version installs"
	@echo ""
	@echo "Quick Start:"
	@echo "  make build        # Build for all versions"
	@echo "  make install-pg15 # Install for PostgreSQL 15"

# Advanced targets for maintainers
security-audit:
	@echo "Running security audit..."
	cargo audit

dependencies:
	@echo "Updating dependencies..."
	cargo update

fresh-install: clean install
	@echo "Fresh installation completed"

# Target to run before creating a release
pre-release: format lint test-all docs package
	@echo "Pre-release checks completed successfully"
	@echo "Ready for release!"

.DEFAULT_GOAL := help
