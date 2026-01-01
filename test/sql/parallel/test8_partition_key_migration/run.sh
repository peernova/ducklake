#!/bin/bash
# Test 8: Partition Key Updates (Row Migration Between Partitions)
# Multiple branches update partition key values, moving rows between partitions

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== TEST 8: Partition Key Migration Across Branches ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_partition_migration..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS parallel_partition_migration WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE parallel_partition_migration;" 2>/dev/null
rm -rf /tmp/parallel_partition_migration 2>/dev/null

echo "Step 1: Setup partitioned orders table and 4 branches..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Running 4 workers migrating rows between partitions IN PARALLEL..."
echo "  - Migrate_A: Moves 'pending' orders to 'processing'"
echo "  - Migrate_B: Moves 'processing' orders to 'shipped'"
echo "  - Migrate_C: Moves 'shipped' orders to 'delivered'"
echo "  - Migrate_D: Moves ALL orders to 'archived'"
echo ""

$DUCKDB < "$DIR/worker_a.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[A] /' &
PID_A=$!

$DUCKDB < "$DIR/worker_b.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[B] /' &
PID_B=$!

$DUCKDB < "$DIR/worker_c.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[C] /' &
PID_C=$!

$DUCKDB < "$DIR/worker_d.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[D] /' &
PID_D=$!

echo "Waiting for all workers..."
wait $PID_A
wait $PID_B
wait $PID_C
wait $PID_D

echo ""
echo "Step 3: Verify partition distributions are isolated..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
