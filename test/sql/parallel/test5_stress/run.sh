#!/bin/bash
# Test 5: High Concurrency Stress Test (6 Workers)
# Tests maximum parallel load with mixed operations

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== TEST 5: High Concurrency Stress Test (6 Workers) ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_stress..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS parallel_stress WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE parallel_stress;" 2>/dev/null
rm -rf /tmp/parallel_stress 2>/dev/null

echo "Step 1: Setup transactions and 6 branches..."
$DUCKDB < "$DIR/setup.sql" 2>/dev/null

echo ""
echo "Step 2: Launching 6 workers IN PARALLEL..."
echo "  - W1: Heavy INSERTs (5 new rows)"
echo "  - W2: Heavy DELETEs (3 deletions + 1 bonus)"
echo "  - W3: Heavy UPDATEs (double deposits, prefix accounts)"
echo "  - W4: Schema change (add 2 columns + data)"
echo "  - W5: Mixed ops with cross-branch queries"
echo "  - W6: Bulk delete + 10 inserts"
echo ""

$DUCKDB < "$DIR/worker_1.sql" 2>/dev/null | sed 's/^/[W1] /' &
PID_1=$!

$DUCKDB < "$DIR/worker_2.sql" 2>/dev/null | sed 's/^/[W2] /' &
PID_2=$!

$DUCKDB < "$DIR/worker_3.sql" 2>/dev/null | sed 's/^/[W3] /' &
PID_3=$!

$DUCKDB < "$DIR/worker_4.sql" 2>/dev/null | sed 's/^/[W4] /' &
PID_4=$!

$DUCKDB < "$DIR/worker_5.sql" 2>/dev/null | sed 's/^/[W5] /' &
PID_5=$!

$DUCKDB < "$DIR/worker_6.sql" 2>/dev/null | sed 's/^/[W6] /' &
PID_6=$!

echo "Waiting for all 6 workers..."
wait $PID_1
wait $PID_2
wait $PID_3
wait $PID_4
wait $PID_5
wait $PID_6

echo ""
echo "Step 3: Verify all branches..."
$DUCKDB < "$DIR/verify.sql" 2>/dev/null
