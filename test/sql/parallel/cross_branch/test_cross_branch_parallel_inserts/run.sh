#!/bin/bash
# Race Condition Test: Verify branch isolation under parallel commits

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== RACE CONDITION TEST ==="
echo "Each branch does 5 sequential INSERTs in parallel"
echo "Expected: Each branch should have 6 rows (1 main + 5 inserts)"
echo ""

# Create fresh database
echo "Creating fresh database..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS race_test WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE race_test;" 2>/dev/null
rm -rf /tmp/race_test 2>/dev/null

echo "Step 1: Setup..."
$DUCKDB < "$DIR/setup.sql" 2>/dev/null

echo ""
echo "Step 2: Running 4 workers IN PARALLEL..."

$DUCKDB < "$DIR/worker_a.sql" 2>/dev/null | sed 's/^/[A] /' &
PID_A=$!

$DUCKDB < "$DIR/worker_b.sql" 2>/dev/null | sed 's/^/[B] /' &
PID_B=$!

$DUCKDB < "$DIR/worker_c.sql" 2>/dev/null | sed 's/^/[C] /' &
PID_C=$!

$DUCKDB < "$DIR/worker_d.sql" 2>/dev/null | sed 's/^/[D] /' &
PID_D=$!

echo "Waiting for all workers..."
wait $PID_A
wait $PID_B
wait $PID_C
wait $PID_D

echo ""
echo "Step 3: Verify results..."
$DUCKDB < "$DIR/verify.sql" 2>/dev/null
