#!/bin/bash

# Test: Compact vs Delete conflict - both operations take time

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKLAKE_DIR="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake"
DUCKDB_PATH="$DUCKLAKE_DIR/build/release/duckdb --unsigned"

echo "=== Test 20v4: Compact vs Delete Conflict ==="

for run in 1 2 3 4 5; do
    echo ""
    echo "--- Run $run ---"

    # Cleanup
    rm -rf /tmp/compact_delete_test
    docker exec ducklake-postgres-test psql -U postgres -c "DROP DATABASE IF EXISTS compact_delete_test;" 2>/dev/null || true
    docker exec ducklake-postgres-test psql -U postgres -c "CREATE DATABASE compact_delete_test;" 2>/dev/null || true

    # Setup
    $DUCKDB_PATH < "$SCRIPT_DIR/setup_large.sql" 2>&1 | grep "msg" || true

    # Start BOTH at exact same time
    $DUCKDB_PATH < "$SCRIPT_DIR/worker_compact_v2.sql" > /tmp/worker_compact.log 2>&1 &
    PID_COMPACT=$!
    $DUCKDB_PATH < "$SCRIPT_DIR/worker_delete_slow.sql" > /tmp/worker_delete.log 2>&1 &
    PID_DELETE=$!

    wait $PID_COMPACT
    STATUS_COMPACT=$?
    wait $PID_DELETE
    STATUS_DELETE=$?

    echo "Compact=$STATUS_COMPACT, Delete=$STATUS_DELETE"

    # Check for conflict
    if [ $STATUS_COMPACT -ne 0 ] || [ $STATUS_DELETE -ne 0 ]; then
        if grep -qi "another transaction\|conflict" /tmp/worker_compact.log /tmp/worker_delete.log 2>/dev/null; then
            echo ""
            echo "*** CONFLICT DETECTED! ***"
            echo ""
            echo "Compact error:"
            grep -i "another transaction\|conflict\|Cannot" /tmp/worker_compact.log 2>/dev/null || echo "(none)"
            echo ""
            echo "Delete error:"
            grep -i "another transaction\|conflict\|Cannot" /tmp/worker_delete.log 2>/dev/null || echo "(none)"
            echo ""
            echo "=== TEST PASSED ==="
            exit 0
        fi
    fi
done

echo ""
echo "No conflict detected in 5 runs - operations executed sequentially"
