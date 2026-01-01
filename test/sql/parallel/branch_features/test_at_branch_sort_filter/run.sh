#!/bin/bash
# AT BRANCH Sort and Filter Test
# Tests ORDER BY and WHERE clauses with AT BRANCH syntax across different branches

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== AT BRANCH SORT AND FILTER TEST ==="
echo "Testing ORDER BY and WHERE with AT (BRANCH => 'name') syntax"
echo "Branches: main, branch_discount (10% off), branch_restock (2x stock), branch_new_items (+3 items)"
echo ""

# Cleanup
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS at_branch_test WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE at_branch_test;" 2>/dev/null
rm -rf /tmp/at_branch_test 2>/dev/null

echo "Step 1: Setup..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Test ORDER BY with AT BRANCH..."
$DUCKDB < "$DIR/test_sort.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 3: Test WHERE filters with AT BRANCH..."
$DUCKDB < "$DIR/test_filter.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 4: Verify..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
