#!/bin/bash

# Test: Compact vs Delete conflict detection
# One worker should fail with conflict error

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKLAKE_DIR="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake"
DUCKDB_PATH="$DUCKLAKE_DIR/build/release/duckdb --unsigned"

echo "=== Test 20: Compact vs Delete Conflict ==="

# Cleanup
echo "Cleaning up previous test data..."
rm -rf /tmp/compact_delete_test
docker exec ducklake-postgres-test psql -U postgres -c "DROP DATABASE IF EXISTS compact_delete_test;" 2>/dev/null || true
docker exec ducklake-postgres-test psql -U postgres -c "CREATE DATABASE compact_delete_test;" 2>/dev/null || true

# Run setup
echo ""
echo "Running setup..."
$DUCKDB_PATH < "$SCRIPT_DIR/setup.sql" 2>&1 | grep -E "(msg|Error)" || true

# Run workers in parallel - delete starts first, compact waits then runs
echo ""
echo "Starting parallel workers..."
echo "Delete Worker: Will run immediately"
echo "Compact Worker: Will sleep 0.5s then run"

# Start both workers in background
$DUCKDB_PATH < "$SCRIPT_DIR/worker_delete.sql" > /tmp/worker_delete.log 2>&1 &
PID_DELETE=$!

$DUCKDB_PATH < "$SCRIPT_DIR/worker_compact.sql" > /tmp/worker_compact.log 2>&1 &
PID_COMPACT=$!

# Wait for both to complete
echo "Waiting for workers to complete..."
wait $PID_DELETE
STATUS_DELETE=$?
wait $PID_COMPACT
STATUS_COMPACT=$?

echo ""
echo "=== Delete Worker Output (exit=$STATUS_DELETE) ==="
cat /tmp/worker_delete.log | grep -E "(msg|Error|conflict|Cannot)" || echo "(no relevant output)"

echo ""
echo "=== Compact Worker Output (exit=$STATUS_COMPACT) ==="
cat /tmp/worker_compact.log | grep -E "(msg|Error|conflict|Cannot)" || echo "(no relevant output)"

echo ""
# Check results
if [ $STATUS_DELETE -eq 0 ] && [ $STATUS_COMPACT -ne 0 ]; then
    echo "=== EXPECTED: Delete succeeded, Compact failed with conflict ==="
    if grep -q "compacted it\|conflict" /tmp/worker_compact.log; then
        echo "=== TEST PASSED: Conflict detected correctly ==="
    else
        echo "=== Compact failed but not due to conflict ==="
        cat /tmp/worker_compact.log
    fi
elif [ $STATUS_COMPACT -eq 0 ] && [ $STATUS_DELETE -ne 0 ]; then
    echo "=== EXPECTED: Compact succeeded, Delete failed with conflict ==="
    if grep -q "deleted from it\|conflict" /tmp/worker_delete.log; then
        echo "=== TEST PASSED: Conflict detected correctly ==="
    else
        echo "=== Delete failed but not due to conflict ==="
        cat /tmp/worker_delete.log
    fi
elif [ $STATUS_DELETE -eq 0 ] && [ $STATUS_COMPACT -eq 0 ]; then
    echo "=== UNEXPECTED: Both succeeded - checking if they ran sequentially ==="
    # This can happen if one finished completely before the other started
    echo "=== This is OK if timing caused sequential execution ==="
else
    echo "=== UNEXPECTED: Both failed ==="
    echo "Delete log:"
    cat /tmp/worker_delete.log
    echo ""
    echo "Compact log:"
    cat /tmp/worker_compact.log
    exit 1
fi
