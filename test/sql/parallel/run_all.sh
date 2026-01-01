#!/bin/bash
# Run all parallel tests

DIR="$(dirname "$0")"

echo "=========================================="
echo "   DUCKLAKE PARALLEL TESTS SUITE"
echo "=========================================="
echo ""

# Test 1: Basic parallel (existing)
echo ">>> Running Test 1: Basic Parallel..."
"$DIR/run_parallel.sh"
echo ""
echo "=========================================="
echo ""

# Test 2: Nested branches
echo ">>> Running Test 2: Nested Branches..."
"$DIR/test2_nested/run.sh"
echo ""
echo "=========================================="
echo ""

# Test 3: Schema changes
echo ">>> Running Test 3: Concurrent Schema Changes..."
"$DIR/test3_schema/run.sh"
echo ""
echo "=========================================="
echo ""

# Test 4: Cross-branch queries
echo ">>> Running Test 4: Cross-Branch Queries..."
"$DIR/test4_cross_query/run.sh"
echo ""
echo "=========================================="
echo ""

# Test 5: Stress test
echo ">>> Running Test 5: High Concurrency Stress..."
"$DIR/test5_stress/run.sh"
echo ""
echo "=========================================="
echo ""

echo "All parallel tests completed!"
