#!/bin/sh

NODES="
10.1.0.11:7001 
10.1.0.12:7002 
10.1.0.13:7003
10.1.0.14:7004 
10.1.0.15:7005 
10.1.0.16:7006
"

# Create the cluster using redis-cli
echo "Creating Redis Cluster..."
echo "yes" | redis-cli --cluster create $NODES --cluster-replicas 1
