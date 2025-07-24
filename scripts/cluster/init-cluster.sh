#!/bin/bash

# Redis Cluster Initialization Script
# This script creates a Redis cluster automatically with proper error handling

set -e

echo "Starting Redis cluster initialization..."

# Cluster configuration
CLUSTER_NODES="${CLUSTER_NODES:-10.1.0.11:7001 10.1.0.12:7002 10.1.0.13:7003 10.1.0.14:7004 10.1.0.15:7005 10.1.0.16:7006}"
REPLICAS_PER_MASTER="${REPLICAS_PER_MASTER:-1}"

# Function to test node connectivity
test_node() {
    local host=$1
    local port=$2
    local max_attempts=30
    local attempt=1
    
    echo "Checking node $host:$port..."
    
    while [ $attempt -le $max_attempts ]; do
        if redis-cli -h $host -p $port ping > /dev/null 2>&1; then
            echo "Node $host:$port is ready"
            return 0
        else
            echo "Attempt $attempt/$max_attempts: Node $host:$port not ready, waiting..."
            sleep 2
            ((attempt++))
        fi
    done
    
    echo "ERROR: Node $host:$port failed to become ready within timeout"
    return 1
}

# Wait for all nodes to be ready
echo "Waiting for all Redis nodes to be ready..."
all_ready=true
for node in $CLUSTER_NODES; do
    host=$(echo $node | cut -d: -f1)
    port=$(echo $node | cut -d: -f2)
    
    if ! test_node $host $port; then
        all_ready=false
    fi
done

if [ "$all_ready" = false ]; then
    echo "ERROR: Not all nodes are ready. Cluster initialization failed."
    exit 1
fi

echo "All nodes are ready. Creating cluster..."

# Create the cluster with error handling
if redis-cli --cluster create $CLUSTER_NODES \
    --cluster-replicas $REPLICAS_PER_MASTER \
    --cluster-yes; then
    
    echo "Redis cluster created successfully!"
    
    # Verify cluster status
    first_node=$(echo $CLUSTER_NODES | cut -d' ' -f1)
    host=$(echo $first_node | cut -d: -f1)
    port=$(echo $first_node | cut -d: -f2)
    
    echo "Cluster status:"
    redis-cli -h $host -p $port cluster info
    echo ""
    echo "Cluster nodes:"
    redis-cli -h $host -p $port cluster nodes
    
    echo "Redis cluster initialization completed successfully!"
else
    echo "ERROR: Failed to create Redis cluster"
    
    # Debug information
    echo "Debug: Checking individual nodes..."
    for node in $CLUSTER_NODES; do
        host=$(echo $node | cut -d: -f1)
        port=$(echo $node | cut -d: -f2)
        echo "Node $host:$port status:"
        redis-cli -h $host -p $port info server | head -5 || echo "  Failed to connect"
    done
    
    exit 1
fi
