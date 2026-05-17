# Redis FDW Makefile

.PHONY: help build build-release install test test-all test-unit \
        clean format lint check setup-redis setup-cluster cleanup-redis redis-status \
        test-pg14 test-pg15 test-pg16 test-pg17 stop-pg

# Default PG version for single-target commands
PG ?= pg14

# Default target
help:
	@echo "Redis FDW - Available Make Targets:"
	@echo ""
	@echo "  make test-all        Run ALL integration tests (starts Redis + Cluster, runs tests, cleans up)"
	@echo "  make test            Run tests for default PG version (PG=$(PG))"
	@echo "  make test PG=pg16    Run tests for a specific PG version"
	@echo "  make test-unit       Run cargo check + clippy (no Redis required)"
	@echo ""
	@echo "  make setup-redis     Start Redis single-node + cluster"
	@echo "  make setup-cluster   Alias for setup-redis (starts both)"
	@echo "  make cleanup-redis   Stop and remove all Redis containers"
	@echo "  make redis-status    Show running Redis containers"
	@echo "  make stop-pg         Stop stale pgrx test PostgreSQL instance"
	@echo ""
	@echo "  make build           cargo build (debug)"
	@echo "  make build-release   cargo build --release"
	@echo "  make format          cargo fmt"
	@echo "  make lint            cargo clippy"
	@echo "  make clean           cargo clean"

# ─── Build ────────────────────────────────────────────────────────────────────

build:
	cargo build

build-release:
	cargo build --release

install:
	cargo pgrx install --release

# ─── Testing ──────────────────────────────────────────────────────────────────

# Run tests for the default PG version (override with PG=pg16 etc.)
test:
	cargo pgrx test $(PG)

test-pg14:
	cargo pgrx test pg14

test-pg15:
	cargo pgrx test pg15

test-pg16:
	cargo pgrx test pg16

test-pg17:
	cargo pgrx test pg17

# Quick compile + lint check, no Redis needed
test-unit:
	cargo check --features $(PG)
	cargo clippy --all-targets --features $(PG)

# Full integration: start infra → run tests → cleanup
test-all: setup-redis stop-pg
	@echo "=== Running all tests for $(PG) ==="
	@cargo pgrx test $(PG); EXIT_CODE=$$?; \
	$(MAKE) cleanup-redis; \
	exit $$EXIT_CODE

# ─── Redis Infrastructure ─────────────────────────────────────────────────────

COMPOSE_FILE := docker-compose.cluster-test.yml
PG_DATA_DIR := $(shell pwd)/target/test-pgdata/$(subst pg,,$(PG))

# Stop any stale pgrx test PostgreSQL instance
stop-pg:
	@if [ -f "$(PG_DATA_DIR)/postmaster.pid" ]; then \
		/usr/lib/postgresql/$(subst pg,,$(PG))/bin/pg_ctl stop -D "$(PG_DATA_DIR)" 2>/dev/null || \
		rm -f "$(PG_DATA_DIR)/postmaster.pid"; \
	fi

setup-redis: cleanup-redis
	@echo "Starting Redis standalone + cluster via docker compose..."
	docker compose -f $(COMPOSE_FILE) up -d --wait
	@echo "Waiting for cluster init to complete..."
	@for i in $$(seq 1 20); do \
		if docker exec redis-cluster-test-1 redis-cli -p 7001 cluster info 2>/dev/null | grep -q 'cluster_state:ok'; then \
			echo "=== Redis cluster is operational ==="; \
			break; \
		fi; \
		if [ $$i -eq 20 ]; then echo "ERROR: cluster not ready"; exit 1; fi; \
		sleep 2; \
	done

cleanup-redis:
	@docker compose -f $(COMPOSE_FILE) down -v 2>/dev/null || true
	@docker rm -f redis-server redis-test 2>/dev/null || true
	@docker ps -a --filter "name=redis-cluster-test" --format "{{.Names}}" | xargs -r docker rm -f 2>/dev/null || true
	@echo "Redis containers removed."

redis-status:
	@docker ps --filter "name=redis" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

setup-cluster: setup-redis

# ─── Development ──────────────────────────────────────────────────────────────

format:
	cargo fmt

lint:
	cargo clippy --all-targets --features $(PG)

check:
	cargo check --features $(PG)

clean:
	cargo clean

.DEFAULT_GOAL := help
