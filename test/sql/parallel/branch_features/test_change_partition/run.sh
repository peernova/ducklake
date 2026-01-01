#!/bin/bash
# Change Partition on Branches Test
# Tests that partition key can be changed on different branches independently

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== CHANGE PARTITION ON BRANCHES TEST ==="
echo "Testing different partition keys on different branches"
echo "main: partition by event_type"
echo "partition_by_region: partition by region"
echo "partition_by_date: partition by event_date"
echo ""

PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS change_partition_test WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE change_partition_test;" 2>/dev/null
rm -rf /tmp/change_partition_test 2>/dev/null

echo "Step 1: Setup..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Verify..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
