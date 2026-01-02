#!/bin/bash
# Test 18: Parallel MERGE Operations
# Tests MERGE INTO across multiple branches concurrently

DUCKDB_BIN="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb"
DIR="$(dirname "$0")"

run_duckdb() {
    "$DUCKDB_BIN" -unsigned "$@"
}

echo "=== TEST 18: Parallel MERGE Operations ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_merge..."
docker exec ducklake-postgres-test psql -U postgres -c "DROP DATABASE IF EXISTS parallel_merge WITH (FORCE);" 2>/dev/null
docker exec ducklake-postgres-test psql -U postgres -c "CREATE DATABASE parallel_merge;" 2>/dev/null
rm -rf /tmp/parallel_merge 2>/dev/null

echo "Step 1: Setup products table and branches..."
run_duckdb < "$DIR/setup.sql"

echo ""
echo "Step 2: Running 3 workers IN PARALLEL with different MERGE operations..."
echo "  - branch_a: UPDATE products 1,2 + INSERT 6,7"
echo "  - branch_b: UPDATE products 3,4 + INSERT 8,9"
echo "  - branch_c: DELETE product 5 + INSERT 10,11"
echo ""

# Run workers truly in parallel - postgres as metadata store allows concurrent access
run_duckdb < "$DIR/worker_a.sql" | sed 's/^/[A] /' &
PID_A=$!

run_duckdb < "$DIR/worker_b.sql" | sed 's/^/[B] /' &
PID_B=$!

run_duckdb < "$DIR/worker_c.sql" | sed 's/^/[C] /' &
PID_C=$!

echo "Waiting for all workers..."
wait $PID_A
wait $PID_B
wait $PID_C

echo ""
echo "Step 3: Verify isolation..."
run_duckdb < "$DIR/verify.sql"
