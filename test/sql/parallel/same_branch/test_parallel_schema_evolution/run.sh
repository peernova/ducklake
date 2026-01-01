#!/bin/bash
# Same Branch Schema Evolution Test
# Tests schema evolution with sequential ALTER TABLE operations
# Note: Parallel ALTER TABLE on same table causes conflicts - only one wins
# This test runs sequentially to verify schema evolution works correctly

DUCKDB="/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/duckdb -unsigned"
DIR="$(dirname "$0")"

echo "=== SAME BRANCH SCHEMA EVOLUTION TEST ==="
echo "Testing sequential column additions (email, phone, age, created_at)"
echo "Note: Parallel ALTER TABLE on same table causes conflicts"
echo ""

PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "DROP DATABASE IF EXISTS schema_evolution_test WITH (FORCE);" 2>/dev/null
PGPASSWORD=postgres /opt/homebrew/Cellar/libpq/18.1/bin/psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE schema_evolution_test;" 2>/dev/null
rm -rf /tmp/schema_evolution_test 2>/dev/null

echo "Step 1: Setup..."
$DUCKDB < "$DIR/setup.sql" 2>&1 | grep -v "^\[DEBUG"

echo ""
echo "Step 2: Running workers SEQUENTIALLY..."

echo ""
echo "[W1] Adding email column..."
$DUCKDB < "$DIR/worker_1.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[W1] /'
E1=$?

echo ""
echo "[W2] Adding phone column..."
$DUCKDB < "$DIR/worker_2.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[W2] /'
E2=$?

echo ""
echo "[W3] Adding age column..."
$DUCKDB < "$DIR/worker_3.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[W3] /'
E3=$?

echo ""
echo "[W4] Adding created_at column..."
$DUCKDB < "$DIR/worker_4.sql" 2>&1 | grep -v "^\[DEBUG" | sed 's/^/[W4] /'
E4=$?

echo ""
echo "Exit codes: W1=$E1, W2=$E2, W3=$E3, W4=$E4"

echo ""
echo "Step 3: Verify..."
$DUCKDB < "$DIR/verify.sql" 2>&1 | grep -v "^\[DEBUG"
