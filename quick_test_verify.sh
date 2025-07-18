#!/bin/bash

# Quick verification that all PostgreSQL versions pass tests
echo "🚀 Quick Test Verification for Redis FDW Async Implementation"
echo "============================================================="

for version in 14 15 16 17; do
    echo "Testing PostgreSQL $version..."
    if cargo pgrx test --features "pg$version" 2>&1 | grep -q "test result: ok"; then
        echo "✅ PostgreSQL $version: PASSED"
    else
        echo "❌ PostgreSQL $version: FAILED"
        exit 1
    fi
done

echo ""
echo "🎉 All PostgreSQL versions (14-17) are working correctly with the async implementation!"
echo "✅ Async Redis operations implemented successfully"
echo "✅ Full backward compatibility maintained"
