#!/bin/bash
# Test 13: NULL Partition Value Handling
# Multiple branches handle rows with NULL partition keys

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== TEST 13: NULL Partition Value Handling ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_null_part..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS parallel_null_part WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE parallel_null_part;" 2>/dev/null
rm -rf /tmp/parallel_null_part 2>/dev/null

echo "Step 1: Setup logs table with NULL categories..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Running 3 workers handling NULL partitions..."
echo "  - Handle_Nulls: Updates NULL rows to have categories"
echo "  - Delete_Nulls: Deletes all NULL partition rows"
echo "  - Keep_Nulls: Keeps NULLs but adds more"
echo ""

$DUCKDB < "$DIR/worker_handle.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[HANDLE] /' &
PID1=$!

$DUCKDB < "$DIR/worker_delete.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[DELETE] /' &
PID2=$!

$DUCKDB < "$DIR/worker_keep.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[KEEP] /' &
PID3=$!

wait $PID1 $PID2 $PID3

echo ""
echo "Step 3: Verify NULL handling isolation..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
