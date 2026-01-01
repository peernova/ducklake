#!/bin/bash
# Same Branch Parallel Test: 4 workers inserting into the SAME branch (main)
# Tests that retry mechanism handles concurrent commits correctly

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== SAME BRANCH PARALLEL INSERT TEST ==="
echo "4 workers each inserting 5 rows into main branch simultaneously"
echo "Expected: 21 total rows (1 setup + 20 from workers)"
echo "Retry mechanism should handle snapshot conflicts"
echo ""

# Create fresh database
echo "Creating fresh database..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS same_branch_test WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE same_branch_test;" 2>/dev/null
rm -rf /tmp/same_branch_test 2>/dev/null

echo "Step 1: Setup..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Running 4 workers IN PARALLEL on SAME branch..."

$DUCKDB < "$DIR/worker_1.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[W1] /' &
PID_1=$!

$DUCKDB < "$DIR/worker_2.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[W2] /' &
PID_2=$!

$DUCKDB < "$DIR/worker_3.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[W3] /' &
PID_3=$!

$DUCKDB < "$DIR/worker_4.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[W4] /' &
PID_4=$!

echo "Waiting for all workers..."
wait $PID_1
EXIT_1=$?
wait $PID_2
EXIT_2=$?
wait $PID_3
EXIT_3=$?
wait $PID_4
EXIT_4=$?

echo ""
echo "Worker exit codes: W1=$EXIT_1, W2=$EXIT_2, W3=$EXIT_3, W4=$EXIT_4"

echo ""
echo "Step 3: Verify results..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
