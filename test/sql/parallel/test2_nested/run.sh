#!/bin/bash
# Test 2: Nested Branches Parallel Test
# Tests parent -> child -> grandchild branch isolation with concurrent operations

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== TEST 2: Nested Branches Parallel Test ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_nested..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS parallel_nested WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE parallel_nested;" 2>/dev/null
rm -rf /tmp/parallel_nested 2>/dev/null

echo "Step 1: Setup nested branch hierarchy..."
$DUCKDB < "$DIR/setup.sql" 2>/dev/null

echo ""
echo "Step 2: Running 4 workers IN PARALLEL on different branch levels..."
echo "  - dev (parent)"
echo "  - feature_a (child)"
echo "  - feature_b (child, sibling)"
echo "  - feature_a_hotfix (grandchild)"
echo ""

$DUCKDB < "$DIR/worker_dev.sql" 2>/dev/null | sed 's/^/[DEV] /' &
PID_DEV=$!

$DUCKDB < "$DIR/worker_feature_a.sql" 2>/dev/null | sed 's/^/[F_A] /' &
PID_FA=$!

$DUCKDB < "$DIR/worker_feature_b.sql" 2>/dev/null | sed 's/^/[F_B] /' &
PID_FB=$!

$DUCKDB < "$DIR/worker_hotfix.sql" 2>/dev/null | sed 's/^/[HOT] /' &
PID_HOT=$!

echo "Waiting for all workers..."
wait $PID_DEV
wait $PID_FA
wait $PID_FB
wait $PID_HOT

echo ""
echo "Step 3: Verify results..."
$DUCKDB < "$DIR/verify.sql" 2>/dev/null
