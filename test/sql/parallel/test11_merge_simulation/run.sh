#!/bin/bash
# Test 11: Merge-Like Scenario Simulation
# Two branches diverge and modify the same table, then we verify both states

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== TEST 11: Merge-Like Scenario Simulation ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_merge_sim..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS parallel_merge_sim WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE parallel_merge_sim;" 2>/dev/null
rm -rf /tmp/parallel_merge_sim 2>/dev/null

echo "Step 1: Setup config table and feature branches..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Running divergent feature branches IN PARALLEL..."
echo "  - Feature_X: Adds feature X settings"
echo "  - Feature_Y: Adds feature Y settings"
echo "  - Hotfix: Fixes critical settings"
echo "  - Main_Cont: Continues development on main"
echo ""

$DUCKDB < "$DIR/worker_feature_x.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[X] /' &
PID_X=$!

$DUCKDB < "$DIR/worker_feature_y.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[Y] /' &
PID_Y=$!

$DUCKDB < "$DIR/worker_hotfix.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[HOT] /' &
PID_HOT=$!

$DUCKDB < "$DIR/worker_main.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[MAIN] /' &
PID_MAIN=$!

echo "Waiting for all workers..."
wait $PID_X
wait $PID_Y
wait $PID_HOT
wait $PID_MAIN

echo ""
echo "Step 3: Verify divergent states (pre-merge verification)..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
