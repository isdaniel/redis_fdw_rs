#!/bin/bash

set -e

# Configurable version
REDIS_VERSION="${REDIS_VERSION:-7-alpine}"
SUBNET="10.1.0.0/24"
NETWORK_NAME="redis-cluster-test"
REPLICAS_PER_MASTER=1

# IPs and ports
NODES=(
  "1 10.1.0.11 7001 17001"
  "2 10.1.0.12 7002 17002"
  "3 10.1.0.13 7003 17003"
  "4 10.1.0.14 7004 17004"
  "5 10.1.0.15 7005 17005"
  "6 10.1.0.16 7006 17006"
)

echo "Creating Docker network $NETWORK_NAME..."
docker network inspect "$NETWORK_NAME" >/dev/null 2>&1 || \
docker network create --subnet=$SUBNET --driver bridge "$NETWORK_NAME"

echo "Starting Redis nodes..."
for NODE in "${NODES[@]}"; do
  read -r ID IP PORT BUS_PORT <<< "$NODE"

  echo "Launching redis-cluster-test-$ID on $IP:$PORT..."

  docker run -d --name "redis-cluster-test-$ID" \
    --hostname "redis-cluster-$ID" \
    --net "$NETWORK_NAME" --ip "$IP" \
    -p "$PORT:$PORT" -p "$BUS_PORT:$BUS_PORT" \
    --sysctl net.core.somaxconn=65535 \
    redis:"$REDIS_VERSION" redis-server \
      --port "$PORT" \
      --cluster-enabled yes \
      --cluster-config-file nodes.conf \
      --cluster-node-timeout 5000 \
      --appendonly yes \
      --bind 0.0.0.0 \
      --protected-mode no \
      --loglevel notice \
      --tcp-keepalive 60 \
      --tcp-backlog 511 \
      --save "" \
      --cluster-announce-hostname "redis-cluster-$ID" \
      --cluster-announce-port "$PORT" \
      --cluster-announce-bus-port "$BUS_PORT"
done

echo "Waiting for all Redis containers to be ready..."

# Function to check if a Redis container is healthy and responding
check_redis_health() {
  local container_name="$1"
  local ip="$2"
  local port="$3"
  local max_attempts=30
  local attempt=1
  
  echo "Checking health of $container_name ($ip:$port)..."
  
  while [ $attempt -le $max_attempts ]; do
    # Check if container is running
    if ! docker ps --format "table {{.Names}}" | grep -q "^$container_name$"; then
      echo "  ❌ Container $container_name is not running (attempt $attempt/$max_attempts)"
      sleep 2
      ((attempt++))
      continue
    fi
    
    # Check if Redis is responding to PING command
    if docker exec "$container_name" redis-cli -p "$port" ping >/dev/null 2>&1; then
      echo "  ✅ $container_name is healthy and responding"
      return 0
    else
      echo "  ⏳ $container_name not ready yet (attempt $attempt/$max_attempts)"
      sleep 2
      ((attempt++))
    fi
  done
  
  echo "  ❌ $container_name failed to become healthy after $max_attempts attempts"
  return 1
}

# Check health of all Redis nodes
all_healthy=true
for NODE in "${NODES[@]}"; do
  read -r ID IP PORT BUS_PORT <<< "$NODE"
  if ! check_redis_health "redis-cluster-test-$ID" "$IP" "$PORT"; then
    all_healthy=false
  fi
done

if [ "$all_healthy" = false ]; then
  echo "❌ Some Redis containers are not healthy. Aborting cluster initialization."
  echo "💡 Try running 'docker logs redis-cluster-test-<ID>' to check container logs"
  exit 1
fi

echo "✅ All Redis containers are healthy and ready!"
echo "Initializing Redis cluster..."
CLUSTER_NODES=$(printf "%s:%s " "${NODES[@]// /:}" | awk '{for(i=2;i<=NF;i+=4) printf $i ":" $(i+1) " "; print ""}')

docker run --rm --name redis-cluster-test-init \
  --net "$NETWORK_NAME" \
  -v "$(pwd)/scripts:/scripts:ro" \
  -e CLUSTER_NODES="$CLUSTER_NODES" \
  -e REPLICAS_PER_MASTER="$REPLICAS_PER_MASTER" \
  redis:"$REDIS_VERSION" /scripts/cluster/init-cluster.sh

echo "✅ Redis cluster is up and running."
