#!/bin/bash
# Test 17: Same Branch Parallel DROP Operations
# Tests multiple workers performing DROP operations on the SAME branch concurrently

DUCKDB_BIN="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb"
DIR="$(dirname "$0")"

run_duckdb() {
    "$DUCKDB_BIN" -unsigned "$@" 2>/dev/null
}

echo "=== TEST 17: Same Branch DROP Operations ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_same_drop..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS parallel_same_drop WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE parallel_same_drop;" 2>/dev/null
rm -rf /tmp/parallel_same_drop 2>/dev/null

echo "Step 1: Setup multiple schemas and tables..."
run_duckdb < "$DIR/setup.sql"

echo ""
echo "Step 2: Running 3 workers IN PARALLEL on SAME BRANCH (dev)..."
echo "  - worker1: DROP TABLE operations"
echo "  - worker2: DROP VIEW operations"
echo "  - worker3: DROP SCHEMA operations"
echo ""
echo "This tests retry logic when multiple workers commit to same branch!"
echo ""

run_duckdb < "$DIR/worker1.sql" | sed 's/^/[W1] /' &
PID_1=$!

run_duckdb < "$DIR/worker2.sql" | sed 's/^/[W2] /' &
PID_2=$!

run_duckdb < "$DIR/worker3.sql" | sed 's/^/[W3] /' &
PID_3=$!

echo "Waiting for all workers..."
wait $PID_1
wait $PID_2
wait $PID_3

echo ""
echo "Step 3: Verify final state..."
run_duckdb < "$DIR/verify.sql"
