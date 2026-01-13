#!/bin/bash

# Test: Compact vs Delete conflict detection - version 3 (larger data)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKLAKE_DIR="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake"
DUCKDB_PATH="$DUCKLAKE_DIR/build/release/duckdb --unsigned"

echo "=== Test 20v3: Compact vs Delete Conflict (large data) ==="

# Cleanup
echo "Cleaning up previous test data..."
rm -rf /tmp/compact_delete_test
docker exec ducklake-postgres-test psql -U postgres -c "DROP DATABASE IF EXISTS compact_delete_test;" 2>/dev/null || true
docker exec ducklake-postgres-test psql -U postgres -c "CREATE DATABASE compact_delete_test;" 2>/dev/null || true

# Run setup with larger data
echo ""
echo "Running setup (larger dataset for longer compaction)..."
$DUCKDB_PATH < "$SCRIPT_DIR/setup_large.sql" 2>&1 | grep -E "(msg|Error)" || true

echo ""
echo "Starting parallel workers simultaneously..."

# Start compact first (it takes longer)
$DUCKDB_PATH < "$SCRIPT_DIR/worker_compact_v2.sql" > /tmp/worker_compact.log 2>&1 &
PID_COMPACT=$!

# Start delete immediately after
$DUCKDB_PATH < "$SCRIPT_DIR/worker_delete_v2.sql" > /tmp/worker_delete.log 2>&1 &
PID_DELETE=$!

# Wait for both
echo "Waiting for workers to complete..."
wait $PID_COMPACT
STATUS_COMPACT=$?
wait $PID_DELETE
STATUS_DELETE=$?

echo ""
echo "=== Results ==="
echo "Compact exit code: $STATUS_COMPACT"
echo "Delete exit code: $STATUS_DELETE"

echo ""
echo "=== Compact Worker Output ==="
cat /tmp/worker_compact.log | grep -v "DEBUG" | head -20

echo ""
echo "=== Delete Worker Output ==="
cat /tmp/worker_delete.log | grep -v "DEBUG" | head -20

# Analyze results
echo ""
echo "=== Analysis ==="
if [ $STATUS_COMPACT -ne 0 ]; then
    echo "Compact FAILED"
    if grep -qi "conflict\|compacted it\|deleted from it\|another transaction" /tmp/worker_compact.log; then
        echo "Reason: CONFLICT DETECTED"
        grep -i "conflict\|another transaction\|deleted from it" /tmp/worker_compact.log | head -3
        echo ""
        echo "=== TEST PASSED: Conflict detection works! ==="
    fi
elif [ $STATUS_DELETE -ne 0 ]; then
    echo "Delete FAILED"
    if grep -qi "conflict\|compacted it\|deleted from it\|another transaction" /tmp/worker_delete.log; then
        echo "Reason: CONFLICT DETECTED"
        grep -i "conflict\|another transaction\|compacted it" /tmp/worker_delete.log | head -3
        echo ""
        echo "=== TEST PASSED: Conflict detection works! ==="
    fi
else
    echo "Both succeeded - one completed before the other started (sequential execution)"
fi
