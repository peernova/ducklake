#!/bin/bash
# Test 15: Time-Travel Queries During Active Writes
# Query historical snapshots while other processes commit new data

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== TEST 15: Time-Travel Queries During Writes ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_timetravel..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS parallel_timetravel WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE parallel_timetravel;" 2>/dev/null
rm -rf /tmp/parallel_timetravel 2>/dev/null

echo "Step 1: Setup history table with initial snapshots..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Running time-travel readers and writers IN PARALLEL..."
echo "  - Writer_Fast: Rapid inserts (5 commits)"
echo "  - Writer_Slow: Slow updates with deletes"
echo "  - Reader_History: Reads historical snapshots"
echo "  - Reader_Branch: Reads across branches at different points"
echo ""

$DUCKDB < "$DIR/worker_writer_fast.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[FAST] /' &
PID1=$!

$DUCKDB < "$DIR/worker_writer_slow.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[SLOW] /' &
PID2=$!

$DUCKDB < "$DIR/worker_reader_history.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[HIST] /' &
PID3=$!

$DUCKDB < "$DIR/worker_reader_branch.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[BRANCH] /' &
PID4=$!

wait $PID1 $PID2 $PID3 $PID4

echo ""
echo "Step 3: Verify time-travel consistency..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
