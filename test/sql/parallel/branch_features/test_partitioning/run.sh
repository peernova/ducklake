#!/bin/bash
# Partitioning with Branches Test
# Tests that partitioned tables work correctly with branch operations

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== PARTITIONING WITH BRANCHES TEST ==="
echo "Testing partitioned table (by region) with multiple branches"
echo "Branches: main, us_branch (+2 US), eu_branch (+3 EU), asia_branch (10% price increase)"
echo ""

PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS partition_test WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE partition_test;" 2>/dev/null
rm -rf /tmp/partition_test 2>/dev/null

echo "Step 1: Setup..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Verify..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
