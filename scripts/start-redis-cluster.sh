#!/bin/bash

set -e

# Redis Cluster test setup using --net=host so nodes can communicate directly
# via 127.0.0.1 and tests can also connect via 127.0.0.1:7001-7006.

REDIS_VERSION="${REDIS_VERSION:-7-alpine}"

PORTS=(7001 7002 7003 7004 7005 7006)

echo "Starting Redis Cluster nodes (host network mode)..."
for i in "${!PORTS[@]}"; do
  PORT=${PORTS[$i]}
  ID=$((i + 1))

  echo "Launching redis-cluster-test-$ID on port $PORT..."

  docker run -d --name "redis-cluster-test-$ID" --net=host \
    redis:"$REDIS_VERSION" redis-server \
      --port "$PORT" \
      --cluster-enabled yes \
      --cluster-config-file "nodes-$ID.conf" \
      --cluster-node-timeout 5000 \
      --appendonly yes \
      --bind 0.0.0.0 \
      --protected-mode no \
      --loglevel notice \
      --save "" \
      --cluster-announce-ip 127.0.0.1 \
      --cluster-announce-port "$PORT" \
      --cluster-announce-bus-port "$((PORT + 10000))"
done

echo "Waiting for Redis nodes to be ready..."
for i in "${!PORTS[@]}"; do
  PORT=${PORTS[$i]}
  ID=$((i + 1))
  for attempt in $(seq 1 30); do
    if docker exec "redis-cluster-test-$ID" redis-cli -h 127.0.0.1 -p "$PORT" ping 2>/dev/null | grep -q PONG; then
      echo "  redis-cluster-test-$ID (port $PORT) is ready"
      break
    fi
    if [ "$attempt" -eq 30 ]; then
      echo "  ERROR: redis-cluster-test-$ID failed to start"
      docker logs "redis-cluster-test-$ID" 2>&1 | tail -5
      exit 1
    fi
    sleep 1
  done
done

echo "All nodes ready. Initializing cluster..."
docker exec redis-cluster-test-1 redis-cli --cluster create \
  127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 \
  127.0.0.1:7004 127.0.0.1:7005 127.0.0.1:7006 \
  --cluster-replicas 1 --cluster-yes

echo "Verifying cluster state..."
for i in $(seq 1 15); do
  STATE=$(docker exec redis-cluster-test-1 redis-cli -h 127.0.0.1 -p 7001 cluster info 2>/dev/null | grep cluster_state || true)
  if echo "$STATE" | grep -q 'cluster_state:ok'; then
    echo "Redis Cluster is fully operational."
    docker exec redis-cluster-test-1 redis-cli -h 127.0.0.1 -p 7001 cluster nodes
    exit 0
  fi
  echo "  Waiting for cluster to stabilize... (attempt $i/15)"
  sleep 2
done

echo "ERROR: Cluster did not become operational in time"
exit 1
