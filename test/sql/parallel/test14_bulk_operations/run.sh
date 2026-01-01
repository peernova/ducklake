#!/bin/bash
# Test 14: Bulk Operations (INSERT SELECT, batch DELETEs)
# Heavy bulk operations across branches

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== TEST 14: Bulk Operations ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_bulk..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS parallel_bulk WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE parallel_bulk;" 2>/dev/null
rm -rf /tmp/parallel_bulk 2>/dev/null

echo "Step 1: Setup data table with 100 rows..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Running 4 bulk operation workers IN PARALLEL..."
echo "  - Bulk_Insert: INSERT SELECT to double data"
echo "  - Bulk_Delete: DELETE 50% of rows"
echo "  - Bulk_Update: UPDATE all rows"
echo "  - Bulk_Mixed: Complex multi-step bulk ops"
echo ""

$DUCKDB < "$DIR/worker_insert.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[INS] /' &
PID1=$!

$DUCKDB < "$DIR/worker_delete.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[DEL] /' &
PID2=$!

$DUCKDB < "$DIR/worker_update.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[UPD] /' &
PID3=$!

$DUCKDB < "$DIR/worker_mixed.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[MIX] /' &
PID4=$!

wait $PID1 $PID2 $PID3 $PID4

echo ""
echo "Step 3: Verify bulk operation isolation..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
