#!/bin/bash
# Test 3: Concurrent Schema Changes Test
# Tests parallel ADD COLUMN operations on different branches

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== TEST 3: Concurrent Schema Changes Test ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_schema..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS parallel_schema WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE parallel_schema;" 2>/dev/null
rm -rf /tmp/parallel_schema 2>/dev/null

echo "Step 1: Setup base table and branches..."
$DUCKDB < "$DIR/setup.sql" 2>/dev/null

echo ""
echo "Step 2: Running 3 workers with DIFFERENT schema changes IN PARALLEL..."
echo "  - V1: adds status + discount columns"
echo "  - V2: adds shipping_address + tracking_number columns"
echo "  - V3: adds created_at + priority columns, deletes row"
echo ""

$DUCKDB < "$DIR/worker_v1.sql" 2>/dev/null | sed 's/^/[V1] /' &
PID_V1=$!

$DUCKDB < "$DIR/worker_v2.sql" 2>/dev/null | sed 's/^/[V2] /' &
PID_V2=$!

$DUCKDB < "$DIR/worker_v3.sql" 2>/dev/null | sed 's/^/[V3] /' &
PID_V3=$!

echo "Waiting for all schema changes..."
wait $PID_V1
wait $PID_V2
wait $PID_V3

echo ""
echo "Step 3: Verify schema isolation..."
$DUCKDB < "$DIR/verify.sql" 2>/dev/null
