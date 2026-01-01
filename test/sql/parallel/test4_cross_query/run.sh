#!/bin/bash
# Test 4: Cross-Branch Queries During Parallel Operations
# Tests reading from other branches while modifying own branch

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== TEST 4: Cross-Branch Queries During Parallel Operations ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_cross..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS parallel_cross WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE parallel_cross;" 2>/dev/null
rm -rf /tmp/parallel_cross 2>/dev/null

echo "Step 1: Setup inventory and warehouse branches..."
$DUCKDB < "$DIR/setup.sql" 2>/dev/null

echo ""
echo "Step 2: Running 3 workers with CROSS-BRANCH QUERIES IN PARALLEL..."
echo "  - NYC: modifies NYC items, queries LA and main"
echo "  - LA: deletes/inserts, queries CHI and main"
echo "  - CHI: updates to zero, queries NYC, LA, and main"
echo ""

$DUCKDB < "$DIR/worker_nyc.sql" 2>/dev/null | sed 's/^/[NYC] /' &
PID_NYC=$!

$DUCKDB < "$DIR/worker_la.sql" 2>/dev/null | sed 's/^/[LA] /' &
PID_LA=$!

$DUCKDB < "$DIR/worker_chi.sql" 2>/dev/null | sed 's/^/[CHI] /' &
PID_CHI=$!

echo "Waiting for all workers..."
wait $PID_NYC
wait $PID_LA
wait $PID_CHI

echo ""
echo "Step 3: Verify cross-branch consistency..."
$DUCKDB < "$DIR/verify.sql" 2>/dev/null
