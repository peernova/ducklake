#!/bin/bash
# Truly parallel branch operations test
# Runs 3 DuckDB instances AT THE SAME TIME using background processes

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== Step 1: Setup main branch ==="
$DUCKDB < "$DIR/setup_main.sql" 2>/dev/null

echo ""
echo "=== Step 2: Running 3 workers IN PARALLEL ==="
echo "Starting workers at $(date +%T)..."

# Run all 3 workers simultaneously using & (background)
$DUCKDB < "$DIR/worker_branch_a.sql" 2>/dev/null | sed 's/^/[A] /' &
PID_A=$!

$DUCKDB < "$DIR/worker_branch_b.sql" 2>/dev/null | sed 's/^/[B] /' &
PID_B=$!

$DUCKDB < "$DIR/worker_branch_c.sql" 2>/dev/null | sed 's/^/[C] /' &
PID_C=$!

echo "Workers started: A=$PID_A, B=$PID_B, C=$PID_C"
echo "Waiting for all workers to complete..."

# Wait for all to finish
wait $PID_A
wait $PID_B
wait $PID_C

echo ""
echo "All workers completed at $(date +%T)"

echo ""
echo "=== Step 3: Verify results ==="
$DUCKDB < "$DIR/verify_results.sql" 2>/dev/null

echo ""
echo "=== Test complete ==="
