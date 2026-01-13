#!/bin/bash

# Test both directions of conflict

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKLAKE_DIR="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake"
DUCKDB_PATH="$DUCKLAKE_DIR/build/release/duckdb --unsigned"

echo "=== Conflict Detection Test ==="

for run in 1 2 3 4 5 6 7 8 9 10; do
    # Cleanup
    rm -rf /tmp/compact_delete_test
    docker exec ducklake-postgres-test psql -U postgres -c "DROP DATABASE IF EXISTS compact_delete_test;" 2>/dev/null || true
    docker exec ducklake-postgres-test psql -U postgres -c "CREATE DATABASE compact_delete_test;" 2>/dev/null || true

    # Setup with large data
    $DUCKDB_PATH < "$SCRIPT_DIR/setup_large.sql" 2>&1 > /dev/null

    # Run both simultaneously
    $DUCKDB_PATH < "$SCRIPT_DIR/worker_compact_v2.sql" > /tmp/worker_compact.log 2>&1 &
    PID_COMPACT=$!
    $DUCKDB_PATH < "$SCRIPT_DIR/worker_delete_slow.sql" > /tmp/worker_delete.log 2>&1 &
    PID_DELETE=$!

    wait $PID_COMPACT
    STATUS_COMPACT=$?
    wait $PID_DELETE
    STATUS_DELETE=$?

    # Check results
    if [ $STATUS_COMPACT -ne 0 ] && grep -qi "deleted from it" /tmp/worker_compact.log; then
        echo "Run $run: COMPACT FAILED - Delete committed first"
        grep "deleted from it" /tmp/worker_compact.log | head -1
        echo ""
        echo "=== Proved: Compact fails when Delete commits first ==="
        exit 0
    elif [ $STATUS_DELETE -ne 0 ] && grep -qi "compacted it" /tmp/worker_delete.log; then
        echo "Run $run: DELETE FAILED - Compact committed first"
        grep "compacted it" /tmp/worker_delete.log | head -1
        echo ""
        echo "=== Proved: Delete fails when Compact commits first ==="
        exit 0
    else
        echo "Run $run: Both succeeded (sequential) - compact=$STATUS_COMPACT delete=$STATUS_DELETE"
    fi
done

echo ""
echo "Did not catch conflict in 10 runs"
