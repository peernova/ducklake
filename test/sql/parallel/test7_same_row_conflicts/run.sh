#!/bin/bash
# Test 7: Same Row Modified in Multiple Branches
# Multiple branches UPDATE/DELETE the exact same rows simultaneously
# Each branch should succeed independently with its own version

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== TEST 7: Same Row Conflicts Across Branches ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_same_row..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS parallel_same_row WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE parallel_same_row;" 2>/dev/null
rm -rf /tmp/parallel_same_row 2>/dev/null

echo "Step 1: Setup accounts table and 5 branches..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Running 5 workers modifying SAME ROWS IN PARALLEL..."
echo "  - Branch A: Updates row 1 balance to 5000"
echo "  - Branch B: Deletes row 1 entirely"
echo "  - Branch C: Updates row 1 name to 'UPDATED_C'"
echo "  - Branch D: Updates rows 1,2,3 balance +100"
echo "  - Branch E: Deletes rows 1,2 and updates row 3"
echo ""

$DUCKDB < "$DIR/worker_a.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[A] /' &
PID_A=$!

$DUCKDB < "$DIR/worker_b.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[B] /' &
PID_B=$!

$DUCKDB < "$DIR/worker_c.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[C] /' &
PID_C=$!

$DUCKDB < "$DIR/worker_d.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[D] /' &
PID_D=$!

$DUCKDB < "$DIR/worker_e.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[E] /' &
PID_E=$!

echo "Waiting for all workers..."
wait $PID_A
wait $PID_B
wait $PID_C
wait $PID_D
wait $PID_E

echo ""
echo "Step 3: Verify each branch has its own version of the data..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
