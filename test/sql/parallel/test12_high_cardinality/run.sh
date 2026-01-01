#!/bin/bash
# Test 12: High-Cardinality Partitions (15+ partition values)
# Multiple branches operate on different partitions simultaneously

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== TEST 12: High-Cardinality Partitions ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_high_card..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS parallel_high_card WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE parallel_high_card;" 2>/dev/null
rm -rf /tmp/parallel_high_card 2>/dev/null

echo "Step 1: Setup events table with 15 countries as partitions..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Running 5 workers on different partition ranges IN PARALLEL..."
echo "  - Americas: US, CA, MX, BR, AR"
echo "  - Europe: UK, DE, FR, IT, ES"
echo "  - AsiaPac: JP, CN, IN, AU, KR"
echo "  - Global: Updates across ALL partitions"
echo "  - NewRegions: Adds new partition values"
echo ""

$DUCKDB < "$DIR/worker_americas.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[AMER] /' &
PID_AMER=$!

$DUCKDB < "$DIR/worker_europe.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[EUR] /' &
PID_EUR=$!

$DUCKDB < "$DIR/worker_asiapac.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[APAC] /' &
PID_APAC=$!

$DUCKDB < "$DIR/worker_global.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[GLOB] /' &
PID_GLOB=$!

$DUCKDB < "$DIR/worker_new.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[NEW] /' &
PID_NEW=$!

echo "Waiting for all workers..."
wait $PID_AMER
wait $PID_EUR
wait $PID_APAC
wait $PID_GLOB
wait $PID_NEW

echo ""
echo "Step 3: Verify partition isolation with 15+ partitions..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
