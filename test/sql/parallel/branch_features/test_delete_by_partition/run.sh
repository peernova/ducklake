#!/bin/bash
# Delete by Partition Value Test
# Tests deleting data by partition value on different branches

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== DELETE BY PARTITION VALUE TEST ==="
echo "Testing partition-based deletion on different branches"
echo "main: all 12 logs (INFO=5, WARNING=3, ERROR=3, CRITICAL=1)"
echo "no_info: INFO deleted (7 remaining)"
echo "clean_logs: ERROR+WARNING deleted (6 remaining)"
echo "critical_only: only CRITICAL kept (1 remaining)"
echo ""

PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS delete_partition_test WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE delete_partition_test;" 2>/dev/null
rm -rf /tmp/delete_partition_test 2>/dev/null

echo "Step 1: Setup..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Verify..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
