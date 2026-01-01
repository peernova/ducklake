#!/bin/bash
# Race Condition Test: All branches update the SAME row in parallel
# This tests true branch isolation

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== SAME ROW RACE CONDITION TEST ==="
echo "All 4 branches update the SAME row (id=1) with different values"
echo "Expected: Each branch sees its own value (100, 200, 300, 400)"
echo ""

# Create fresh database
echo "Creating fresh database..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS race_same_row WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE race_same_row;" 2>/dev/null
rm -rf /tmp/race_same_row 2>/dev/null

echo "Step 1: Setup..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Running 4 workers IN PARALLEL (all updating same row)..."

$DUCKDB < "$DIR/worker_a.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[A] /' &
PID_A=$!

$DUCKDB < "$DIR/worker_b.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[B] /' &
PID_B=$!

$DUCKDB < "$DIR/worker_c.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[C] /' &
PID_C=$!

$DUCKDB < "$DIR/worker_d.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[D] /' &
PID_D=$!

echo "Waiting for all workers..."
wait $PID_A
wait $PID_B
wait $PID_C
wait $PID_D

echo ""
echo "Step 3: Verify results..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
