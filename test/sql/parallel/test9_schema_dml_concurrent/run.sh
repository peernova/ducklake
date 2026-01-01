#!/bin/bash
# Test 9: Concurrent Schema Changes + DML Operations
# One branch modifies schema while others do heavy DML

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== TEST 9: Concurrent Schema Changes + DML ==="
echo ""

# Create fresh database
echo "Creating fresh database parallel_schema_dml..."
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS parallel_schema_dml WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE parallel_schema_dml;" 2>/dev/null
rm -rf /tmp/parallel_schema_dml 2>/dev/null

echo "Step 1: Setup products table and 4 branches..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Running 4 workers with mixed schema/DML operations IN PARALLEL..."
echo "  - Schema_Worker: ADD COLUMN, DROP COLUMN, repartition"
echo "  - DML_Heavy: 100 INSERTs"
echo "  - DML_Updates: UPDATE all rows multiple times"
echo "  - DML_Deletes: DELETE half the rows, INSERT new ones"
echo ""

$DUCKDB < "$DIR/worker_schema.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[SCHEMA] /' &
PID_SCHEMA=$!

$DUCKDB < "$DIR/worker_dml_heavy.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[HEAVY] /' &
PID_HEAVY=$!

$DUCKDB < "$DIR/worker_dml_updates.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[UPDATE] /' &
PID_UPDATES=$!

$DUCKDB < "$DIR/worker_dml_deletes.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[DELETE] /' &
PID_DELETES=$!

echo "Waiting for all workers..."
wait $PID_SCHEMA
wait $PID_HEAVY
wait $PID_UPDATES
wait $PID_DELETES

echo ""
echo "Step 3: Verify schema and data isolation..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
