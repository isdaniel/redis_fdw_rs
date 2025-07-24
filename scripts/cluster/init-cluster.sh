#!/bin/bash

# Redis Cluster Initialization Script
# This script creates a Redis cluster automatically with proper error handling

set -e

echo "Starting Redis cluster initialization..."

# Cluster configuration
CLUSTER_NODES="${CLUSTER_NODES:-10.1.0.11:7001 10.1.0.12:7002 10.1.0.13:7003 10.1.0.14:7004 10.1.0.15:7005 10.1.0.16:7006}"
REPLICAS_PER_MASTER="${REPLICAS_PER_MASTER:-1}"

# Function to test node connectivity with improved error handling
test_node() {
    local host=$1
    local port=$2
    local max_attempts=60  # Increased from 30
    local attempt=1
    
    echo "Checking node $host:$port..."
    
    while [ $attempt -le $max_attempts ]; do
        # Use timeout to prevent hanging
        if timeout 5 redis-cli -h $host -p $port ping > /dev/null 2>&1; then
            echo "Node $host:$port is ready"
            return 0
        else
            echo "Attempt $attempt/$max_attempts: Node $host:$port not ready, waiting..."
            sleep 3  # Increased from 2 seconds
            ((attempt++))
        fi
    done
    
    echo "ERROR: Node $host:$port failed to become ready within timeout"
    
    # Debug information
    echo "Debug: Attempting to connect to $host:$port with verbose output:"
    timeout 5 redis-cli -h $host -p $port ping || echo "Connection failed"
    
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

# Add a brief delay to ensure nodes are fully stable
echo "Waiting 5 seconds for nodes to stabilize..."
sleep 5

# Create the cluster with error handling and retries
cluster_created=false
max_cluster_attempts=3
cluster_attempt=1

while [ $cluster_attempt -le $max_cluster_attempts ] && [ "$cluster_created" = false ]; do
    echo "Cluster creation attempt $cluster_attempt/$max_cluster_attempts..."
    
    if timeout 60 redis-cli --cluster create $CLUSTER_NODES \
        --cluster-replicas $REPLICAS_PER_MASTER \
        --cluster-yes; then
        
        echo "Redis cluster created successfully!"
        cluster_created=true
        
        # Wait for cluster to stabilize
        echo "Waiting for cluster to stabilize..."
        sleep 10
        
        # Verify cluster status with retry logic
        first_node=$(echo $CLUSTER_NODES | cut -d' ' -f1)
        host=$(echo $first_node | cut -d: -f1)
        port=$(echo $first_node | cut -d: -f2)
        
        echo "Verifying cluster status..."
        for verify_attempt in {1..30}; do
            if timeout 10 redis-cli -h $host -p $port cluster info 2>/dev/null | grep -q "cluster_state:ok"; then
                echo "✅ Cluster verification successful!"
                echo "Cluster status:"
                redis-cli -h $host -p $port cluster info
                echo ""
                echo "Cluster nodes:"
                redis-cli -h $host -p $port cluster nodes
                echo "Redis cluster initialization completed successfully!"
                exit 0
            else
                echo "Verification attempt $verify_attempt/30: Cluster not ready yet..."
                sleep 2
            fi
        done
        
        echo "Warning: Cluster created but verification failed after 30 attempts"
        
    else
        echo "ERROR: Failed to create Redis cluster (attempt $cluster_attempt)"
        ((cluster_attempt++))
        
        if [ $cluster_attempt -le $max_cluster_attempts ]; then
            echo "Waiting 10 seconds before retry..."
            sleep 10
        fi
    fi
done

if [ "$cluster_created" = false ]; then
    echo "ERROR: Failed to create Redis cluster after $max_cluster_attempts attempts"
    
    # Debug information
    echo "Debug: Checking individual nodes..."
    for node in $CLUSTER_NODES; do
        host=$(echo $node | cut -d: -f1)
        port=$(echo $node | cut -d: -f2)
        echo "Node $host:$port status:"
        timeout 5 redis-cli -h $host -p $port info server | head -5 || echo "  Failed to connect"
    done
    
    exit 1
fi
