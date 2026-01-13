#!/bin/bash

# Test: Delete commits first, Compact should fail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKLAKE_DIR="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake"
DUCKDB_PATH="$DUCKLAKE_DIR/build/release/duckdb --unsigned"

echo "=== Test: Delete first, Compact should fail ==="

# Cleanup
rm -rf /tmp/compact_delete_test
docker exec ducklake-postgres-test psql -U postgres -c "DROP DATABASE IF EXISTS compact_delete_test;" 2>/dev/null || true
docker exec ducklake-postgres-test psql -U postgres -c "CREATE DATABASE compact_delete_test;" 2>/dev/null || true

# Setup
echo "Setup..."
$DUCKDB_PATH < "$SCRIPT_DIR/setup_large.sql" 2>&1 | grep "msg" || true

# Delete starts first and runs quickly
$DUCKDB_PATH < "$SCRIPT_DIR/worker_delete_v2.sql" > /tmp/worker_delete.log 2>&1 &
PID_DELETE=$!

# Small delay then start compaction
sleep 0.2
$DUCKDB_PATH < "$SCRIPT_DIR/worker_compact_v2.sql" > /tmp/worker_compact.log 2>&1 &
PID_COMPACT=$!

wait $PID_DELETE
STATUS_DELETE=$?
wait $PID_COMPACT
STATUS_COMPACT=$?

echo ""
echo "Delete exit=$STATUS_DELETE, Compact exit=$STATUS_COMPACT"

echo ""
echo "=== Delete Output ==="
grep -E "msg|Error|conflict|Cannot" /tmp/worker_delete.log 2>/dev/null | grep -v "DEBUG" | head -10

echo ""
echo "=== Compact Output ==="
grep -E "msg|Error|conflict|Cannot|another" /tmp/worker_compact.log 2>/dev/null | grep -v "DEBUG" | head -10

if [ $STATUS_COMPACT -ne 0 ]; then
    if grep -qi "deleted from it\|another transaction" /tmp/worker_compact.log; then
        echo ""
        echo "=== TEST PASSED: Compact failed because delete happened first ==="
    fi
fi
