#!/bin/bash
# Same Branch Parallel UPDATE Test
# 4 workers each updating a DIFFERENT row on the same branch

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== SAME BRANCH PARALLEL UPDATE TEST ==="
echo "4 workers each updating a different row (+50 each)"
echo "Expected: Each row should have value=50"
echo ""

# Create fresh database
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS same_branch_updates WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE same_branch_updates;" 2>/dev/null
rm -rf /tmp/same_branch_updates 2>/dev/null

echo "Step 1: Setup..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Running 4 workers IN PARALLEL..."

$DUCKDB < "$DIR/worker_1.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[W1] /' &
PID_1=$!
$DUCKDB < "$DIR/worker_2.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[W2] /' &
PID_2=$!
$DUCKDB < "$DIR/worker_3.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[W3] /' &
PID_3=$!
$DUCKDB < "$DIR/worker_4.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[W4] /' &
PID_4=$!

wait $PID_1; E1=$?
wait $PID_2; E2=$?
wait $PID_3; E3=$?
wait $PID_4; E4=$?

echo ""
echo "Exit codes: W1=$E1, W2=$E2, W3=$E3, W4=$E4"

echo ""
echo "Step 3: Verify..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
