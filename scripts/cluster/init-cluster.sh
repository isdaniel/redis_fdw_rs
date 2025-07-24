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
    local max_attempts=120  # Increased to 4 minutes
    local attempt=1
    
    echo "Checking node $host:$port..."
    
    while [ $attempt -le $max_attempts ]; do
        # Use timeout to prevent hanging and test basic connectivity first
        if timeout 3 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null; then
            # Port is open, now test Redis
            if timeout 5 redis-cli -h $host -p $port ping > /dev/null 2>&1; then
                echo "Node $host:$port is ready"
                return 0
            else
                echo "Attempt $attempt/$max_attempts: Port $port open but Redis not ready on $host, waiting..."
            fi
        else
            echo "Attempt $attempt/$max_attempts: Port $port not open on $host, waiting..."
        fi
        
        sleep 2
        ((attempt++))
    done
    
    echo "ERROR: Node $host:$port failed to become ready within timeout"
    
    # Enhanced debug information
    echo "Debug information for $host:$port:"
    echo "  - Checking port connectivity:"
    timeout 3 bash -c "echo > /dev/tcp/$host/$port" 2>&1 || echo "    Port not reachable"
    echo "  - Checking Redis ping:"
    timeout 5 redis-cli -h $host -p $port ping 2>&1 || echo "    Redis ping failed"
    echo "  - Checking Redis info:"
    timeout 5 redis-cli -h $host -p $port info server 2>&1 | head -5 || echo "    Redis info failed"
    
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
max_cluster_attempts=5  # Increased attempts
cluster_attempt=1

while [ $cluster_attempt -le $max_cluster_attempts ] && [ "$cluster_created" = false ]; do
    echo "Cluster creation attempt $cluster_attempt/$max_cluster_attempts..."
    
    # First, reset any existing cluster state on all nodes
    echo "Resetting cluster state on all nodes..."
    for node in $CLUSTER_NODES; do
        host=$(echo $node | cut -d: -f1)
        port=$(echo $node | cut -d: -f2)
        
        echo "Resetting node $host:$port..."
        # Try to reset cluster and flush
        timeout 10 redis-cli -h $host -p $port CLUSTER RESET HARD 2>/dev/null || true
        timeout 10 redis-cli -h $host -p $port FLUSHALL 2>/dev/null || true
        sleep 1
    done
    
    echo "Waiting 5 seconds for reset to complete..."
    sleep 5
    
    if timeout 120 redis-cli --cluster create $CLUSTER_NODES \
        --cluster-replicas $REPLICAS_PER_MASTER \
        --cluster-yes; then
        
        echo "Redis cluster created successfully!"
        cluster_created=true
        
        # Wait for cluster to stabilize
        echo "Waiting for cluster to stabilize..."
        sleep 15  # Increased stabilization time
        
        # Verify cluster status with retry logic
        first_node=$(echo $CLUSTER_NODES | cut -d' ' -f1)
        host=$(echo $first_node | cut -d: -f1)
        port=$(echo $first_node | cut -d: -f2)
        
        echo "Verifying cluster status..."
        for verify_attempt in {1..60}; do  # Increased verification attempts
            if timeout 10 redis-cli -h $host -p $port cluster info 2>/dev/null | grep -q "cluster_state:ok"; then
                echo "✅ Cluster verification successful!"
                echo "Cluster status:"
                redis-cli -h $host -p $port cluster info
                echo ""
                echo "Cluster nodes:"
                redis-cli -h $host -p $port cluster nodes
                
                # Additional validation: ensure all nodes are reachable
                echo ""
                echo "Final connectivity check for all nodes:"
                all_nodes_ok=true
                for node in $CLUSTER_NODES; do
                    node_host=$(echo $node | cut -d: -f1)
                    node_port=$(echo $node | cut -d: -f2)
                    if timeout 5 redis-cli -h $node_host -p $node_port ping > /dev/null 2>&1; then
                        echo "  ✅ Node $node_host:$node_port is responsive"
                    else
                        echo "  ❌ Node $node_host:$node_port is not responsive"
                        all_nodes_ok=false
                    fi
                done
                
                if [ "$all_nodes_ok" = true ]; then
                    echo "Redis cluster initialization completed successfully!"
                    exit 0
                else
                    echo "Warning: Some nodes are not responsive, retrying cluster creation..."
                    cluster_created=false
                    break
                fi
            else
                echo "Verification attempt $verify_attempt/60: Cluster not ready yet..."
                sleep 2
            fi
        done
        
        if [ "$cluster_created" = true ]; then
            echo "Warning: Cluster created but final verification failed after 60 attempts"
            cluster_created=false
        fi
        
    else
        echo "ERROR: Failed to create Redis cluster (attempt $cluster_attempt)"
        
        # Debug information for failed attempt
        echo "Debug: Checking individual nodes after failed cluster creation..."
        for node in $CLUSTER_NODES; do
            host=$(echo $node | cut -d: -f1)
            port=$(echo $node | cut -d: -f2)
            echo "Node $host:$port status:"
            timeout 5 redis-cli -h $host -p $port info server 2>&1 | head -5 || echo "  Failed to connect"
            timeout 5 redis-cli -h $host -p $port cluster info 2>&1 || echo "  No cluster info available"
        done
    fi
    
    ((cluster_attempt++))
    
    if [ $cluster_attempt -le $max_cluster_attempts ] && [ "$cluster_created" = false ]; then
        echo "Waiting 15 seconds before retry..."
        sleep 15
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
