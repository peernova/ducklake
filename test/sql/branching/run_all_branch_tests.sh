#!/bin/bash
# ============================================================================
# Run All DuckLake Branching Tests
# ============================================================================
# Usage: ./run_all_branch_tests.sh
# ============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKLAKE_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

echo "=============================================="
echo "DuckLake Branching Test Suite"
echo "=============================================="
echo "DuckLake directory: $DUCKLAKE_DIR"
echo ""
echo "NOTE: Branches are READ-ONLY snapshots."
echo "      All DML goes to main branch."
echo "      Use AT (BRANCH => 'name') in SELECT only."
echo ""

cd "$DUCKLAKE_DIR"

# Build if needed
if [ ! -f "build/release/duckdb" ]; then
    echo "Building DuckLake..."
    make GEN=ninja release
fi

DUCKDB="./build/release/duckdb"
EXTENSION="build/release/extension/ducklake/ducklake.duckdb_extension"

echo "=============================================="
echo "Running Interactive Test..."
echo "=============================================="

rm -rf interactive_test.ducklake interactive_test.ducklake.files

if $DUCKDB -unsigned < "test/sql/branching/test_interactive_branching.sql" > /tmp/interactive_output.txt 2>&1; then
    echo "✅ Interactive test passed"
    echo ""
    echo "Last few lines of output:"
    tail -10 /tmp/interactive_output.txt
else
    echo "❌ Interactive test failed"
    echo ""
    echo "Error output:"
    tail -30 /tmp/interactive_output.txt
    exit 1
fi

# Cleanup
rm -rf interactive_test.ducklake interactive_test.ducklake.files

echo ""
echo "=============================================="
echo "Quick Branch Test..."
echo "=============================================="

rm -rf quick_test.ducklake quick_test.ducklake.files

$DUCKDB -unsigned -c "
LOAD '$EXTENSION';
ATTACH 'ducklake:quick_test.ducklake' AS dl;
USE dl;

-- Create table and initial data
CREATE TABLE products (id INT, name VARCHAR);
INSERT INTO products VALUES (1, 'Widget');

-- Create branch (snapshot at 1 product)
SELECT * FROM ducklake_create_branch('dl', 'snapshot_v1');

-- Add more to main
INSERT INTO products VALUES (2, 'Gadget');

-- Create another branch (snapshot at 2 products)
SELECT * FROM ducklake_create_branch('dl', 'snapshot_v2');

-- Add more to main
INSERT INTO products VALUES (3, 'Gizmo');

-- Verify: main=3, v1=1, v2=2
SELECT 'main', COUNT(*) FROM products AT (BRANCH => 'main')
UNION ALL SELECT 'snapshot_v1', COUNT(*) FROM products AT (BRANCH => 'snapshot_v1')
UNION ALL SELECT 'snapshot_v2', COUNT(*) FROM products AT (BRANCH => 'snapshot_v2');

-- Verify isolation
SELECT 'Test passed!' as result WHERE 
    (SELECT COUNT(*) FROM products AT (BRANCH => 'main')) = 3 AND
    (SELECT COUNT(*) FROM products AT (BRANCH => 'snapshot_v1')) = 1 AND
    (SELECT COUNT(*) FROM products AT (BRANCH => 'snapshot_v2')) = 2;
" && echo "✅ Quick branch test passed" || echo "❌ Quick branch test failed"

rm -rf quick_test.ducklake quick_test.ducklake.files

echo ""
echo "=============================================="
echo "Done!"
echo "=============================================="
