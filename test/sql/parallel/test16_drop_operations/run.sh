#!/bin/bash
# Test 16: Parallel DROP Operations Test
# Tests DROP TABLE, DROP VIEW, DROP SCHEMA across multiple branches concurrently

DUCKDB_BIN="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb"
DIR="$(dirname "$0")"

run_duckdb() {
    "$DUCKDB_BIN" -unsigned "$@" 2>/dev/null
}

echo "=== TEST 16: Branch DROP Operations Isolation ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_drop..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS parallel_drop WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE parallel_drop;" 2>/dev/null
rm -rf /tmp/parallel_drop 2>/dev/null

echo "Step 1: Setup schemas, tables, and views on main..."
run_duckdb < "$DIR/setup.sql"

echo ""
echo "Step 2: Running workers sequentially on different branches..."
echo "  - branch_a: DROP TABLE in sales schema"
echo "  - branch_b: DROP VIEW in analytics schema"
echo "  - branch_c: DROP SCHEMA CASCADE (reporting)"
echo "  - branch_d: Multiple DROP operations"
echo ""

echo "[A] Starting..."
run_duckdb < "$DIR/worker_a.sql" | sed 's/^/[A] /'

echo ""
echo "[B] Starting..."
run_duckdb < "$DIR/worker_b.sql" | sed 's/^/[B] /'

echo ""
echo "[C] Starting..."
run_duckdb < "$DIR/worker_c.sql" | sed 's/^/[C] /'

echo ""
echo "[D] Starting..."
run_duckdb < "$DIR/worker_d.sql" | sed 's/^/[D] /'

echo ""
echo "Step 3: Verify isolation..."
run_duckdb < "$DIR/verify.sql"
