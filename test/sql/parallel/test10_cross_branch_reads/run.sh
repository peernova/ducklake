#!/bin/bash
# Test 10: Cross-Branch Reads During Writes
# Workers read from other branches while performing writes on their own

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== TEST 10: Cross-Branch Reads During Writes ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_cross_reads..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS parallel_cross_reads WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE parallel_cross_reads;" 2>/dev/null
rm -rf /tmp/parallel_cross_reads 2>/dev/null

echo "Step 1: Setup metrics table and 3 branches..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Running 3 workers with cross-branch reads during writes..."
echo "  - Writer_A: Writes data, reads from B and main"
echo "  - Writer_B: Writes data, reads from A and main"
echo "  - Reader_Only: Only reads from A, B, and main continuously"
echo ""

$DUCKDB < "$DIR/worker_a.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[A] /' &
PID_A=$!

$DUCKDB < "$DIR/worker_b.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[B] /' &
PID_B=$!

$DUCKDB < "$DIR/worker_reader.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[READER] /' &
PID_READER=$!

echo "Waiting for all workers..."
wait $PID_A
wait $PID_B
wait $PID_READER

echo ""
echo "Step 3: Verify isolation with cross-reads..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
