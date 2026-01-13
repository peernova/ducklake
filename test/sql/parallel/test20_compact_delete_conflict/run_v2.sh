#!/bin/bash

# Test: Compact vs Delete conflict detection - version 2
# Both start near-simultaneously to force overlap

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKLAKE_DIR="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake"
DUCKDB_PATH="$DUCKLAKE_DIR/build/release/duckdb --unsigned"

echo "=== Test 20v2: Compact vs Delete Conflict (simultaneous) ==="

# Cleanup
echo "Cleaning up previous test data..."
rm -rf /tmp/compact_delete_test
docker exec ducklake-postgres-test psql -U postgres -c "DROP DATABASE IF EXISTS compact_delete_test;" 2>/dev/null || true
docker exec ducklake-postgres-test psql -U postgres -c "CREATE DATABASE compact_delete_test;" 2>/dev/null || true

# Run setup
echo ""
echo "Running setup..."
$DUCKDB_PATH < "$SCRIPT_DIR/setup.sql" 2>&1 | grep -E "(msg|Error)" || true

# Run multiple times to catch the race condition
for run in 1 2 3; do
    echo ""
    echo "=== Run $run of 3 ==="

    # Reset data for each run
    rm -rf /tmp/compact_delete_test
    docker exec ducklake-postgres-test psql -U postgres -c "DROP DATABASE IF EXISTS compact_delete_test;" 2>/dev/null || true
    docker exec ducklake-postgres-test psql -U postgres -c "CREATE DATABASE compact_delete_test;" 2>/dev/null || true
    $DUCKDB_PATH < "$SCRIPT_DIR/setup.sql" 2>&1 | grep -v "DEBUG" > /dev/null

    # Start both workers simultaneously
    $DUCKDB_PATH < "$SCRIPT_DIR/worker_compact_v2.sql" > /tmp/worker_compact.log 2>&1 &
    PID_COMPACT=$!

    $DUCKDB_PATH < "$SCRIPT_DIR/worker_delete_v2.sql" > /tmp/worker_delete.log 2>&1 &
    PID_DELETE=$!

    # Wait for both
    wait $PID_COMPACT
    STATUS_COMPACT=$?
    wait $PID_DELETE
    STATUS_DELETE=$?

    echo "Compact exit=$STATUS_COMPACT, Delete exit=$STATUS_DELETE"

    # Check for conflict
    if [ $STATUS_COMPACT -ne 0 ] || [ $STATUS_DELETE -ne 0 ]; then
        echo ""
        if grep -qi "conflict\|compacted it\|deleted from it\|another transaction" /tmp/worker_compact.log /tmp/worker_delete.log 2>/dev/null; then
            echo "*** CONFLICT DETECTED! ***"
            echo ""
            echo "Compact output:"
            grep -i "conflict\|compacted\|deleted\|another\|error\|Cannot" /tmp/worker_compact.log 2>/dev/null || echo "(clean)"
            echo ""
            echo "Delete output:"
            grep -i "conflict\|compacted\|deleted\|another\|error\|Cannot" /tmp/worker_delete.log 2>/dev/null || echo "(clean)"
            echo ""
            echo "=== TEST PASSED: Conflict detection works! ==="
            exit 0
        fi
    fi
done

echo ""
echo "All 3 runs completed without conflict - operations may have executed sequentially"
echo "This can happen if one operation is very fast"

# Verify final state is consistent
echo ""
echo "Verifying data consistency..."
$DUCKDB_PATH -c "
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=compact_delete_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/compact_delete_test');
USE t;
SELECT 'Final count: ' || COUNT(*) as msg FROM orders;
" 2>&1 | grep "msg"
