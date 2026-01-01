#!/bin/bash
# Test 6: Concurrent UPDATEs on Same Partition Across Branches
# Multiple branches update the same inherited partition data simultaneously

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== TEST 6: Concurrent Partition UPDATEs Across Branches ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_partition_updates..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS parallel_partition_updates WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE parallel_partition_updates;" 2>/dev/null
rm -rf /tmp/parallel_partition_updates 2>/dev/null

echo "Step 1: Setup partitioned sales table and 4 regional branches..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Running 4 workers updating SAME partitions IN PARALLEL..."
echo "  - US_team: Updates US region prices (+10%)"
echo "  - EU_team: Updates EU region prices (+20%)"
echo "  - ASIA_team: Updates ASIA region prices (+15%)"
echo "  - GLOBAL_team: Updates ALL regions (+5%)"
echo ""

$DUCKDB < "$DIR/worker_us.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[US] /' &
PID_US=$!

$DUCKDB < "$DIR/worker_eu.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[EU] /' &
PID_EU=$!

$DUCKDB < "$DIR/worker_asia.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[ASIA] /' &
PID_ASIA=$!

$DUCKDB < "$DIR/worker_global.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[GLOBAL] /' &
PID_GLOBAL=$!

echo "Waiting for all workers..."
wait $PID_US
wait $PID_EU
wait $PID_ASIA
wait $PID_GLOBAL

echo ""
echo "Step 3: Verify partition isolation..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
