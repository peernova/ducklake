#!/bin/bash
# Cross-Branch Joins and Multi-Catalog Test
# Tests joining tables across different branches and different catalogs

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=========================================="
echo "  CROSS-BRANCH JOINS & MULTI-CATALOG TEST"
echo "=========================================="
echo ""
echo "Testing:"
echo "  1. Joins within same catalog, same branch"
echo "  2. Joins within same catalog, DIFFERENT branches"
echo "  3. Joins across DIFFERENT catalogs, same branch"
echo "  4. Joins across DIFFERENT catalogs, DIFFERENT branches"
echo "  5. Aggregates comparing branches"
echo "  6. Subqueries with different branches"
echo "  7. Complex 3-way cross-catalog cross-branch joins"
echo ""
echo "Catalogs:"
echo "  - sales: customers, orders (branches: main, promo_branch, cleanup_branch)"
echo "  - inventory: products, stock (branches: main, sale_branch, new_products_branch)"
echo ""
echo "=========================================="

# Cleanup
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS catalog_sales WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS catalog_inventory WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE catalog_sales;" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE catalog_inventory;" 2>/dev/null
rm -rf /tmp/catalog_sales /tmp/catalog_inventory 2>/dev/null

echo ""
echo "Step 1: Setup catalogs and branches..."
echo "----------------------------------------"
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG" | grep -v "^$" | grep -v "SELECT \* FROM" | grep -v "^\t" | grep -v "FROM {" | grep -v "WHERE " | grep -v "JOIN " | grep -v "COALESCE" | grep -v "ORDER BY" | grep -v "LIMIT" | grep -v "LEFT JOIN"

echo ""
echo "Step 2: Running cross-branch join tests..."
echo "----------------------------------------"
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG" | grep -v "^$" | grep -v "SELECT \* FROM" | grep -v "^\t" | grep -v "FROM {" | grep -v "WHERE " | grep -v "COALESCE" | grep -v "LEFT JOIN" | grep -v "^AND " | grep -v "^  AND"

echo ""
echo "=========================================="
echo "  CROSS-BRANCH JOINS TEST COMPLETE"
echo "=========================================="
