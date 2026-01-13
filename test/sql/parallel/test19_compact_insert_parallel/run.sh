#!/bin/bash

# Parallel test: Compaction and Insert on the SAME branch
# Tests that compaction doesn't interfere with concurrent inserts on the same branch

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKLAKE_DIR="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake"
DUCKDB_PATH="$DUCKLAKE_DIR/build/release/duckdb --unsigned"

echo "=== Test 19: Parallel Compaction + Insert on Same Branch ==="

# Cleanup
echo "Cleaning up previous test data..."
rm -rf /tmp/parallel_compact
docker exec ducklake-postgres-test psql -U postgres -c "DROP DATABASE IF EXISTS parallel_compact;" 2>/dev/null || true
docker exec ducklake-postgres-test psql -U postgres -c "CREATE DATABASE parallel_compact;" 2>/dev/null || true

# Run setup
echo ""
echo "Running setup..."
$DUCKDB_PATH < "$SCRIPT_DIR/setup.sql"

# Run workers in parallel
echo ""
echo "Starting parallel workers on same branch..."
echo "Worker A: Compaction on test_branch"
echo "Worker B: Inserts on test_branch"

# Start both workers in background
$DUCKDB_PATH < "$SCRIPT_DIR/worker_a.sql" > /tmp/worker_a.log 2>&1 &
PID_A=$!

$DUCKDB_PATH < "$SCRIPT_DIR/worker_b.sql" > /tmp/worker_b.log 2>&1 &
PID_B=$!

# Wait for both to complete
echo "Waiting for workers to complete..."
wait $PID_A
STATUS_A=$?
wait $PID_B
STATUS_B=$?

echo ""
echo "=== Worker A (Compaction) Output ==="
cat /tmp/worker_a.log

echo ""
echo "=== Worker B (Insert) Output ==="
cat /tmp/worker_b.log

# Check for failures
if [ $STATUS_A -ne 0 ]; then
    echo "ERROR: Worker A (compaction) failed with exit code $STATUS_A"
    exit 1
fi

if [ $STATUS_B -ne 0 ]; then
    echo "ERROR: Worker B (insert) failed with exit code $STATUS_B"
    exit 1
fi

# Run verification
echo ""
echo "Running verification..."
$DUCKDB_PATH < "$SCRIPT_DIR/verify.sql"

echo ""
echo "=== TEST PASSED ==="
