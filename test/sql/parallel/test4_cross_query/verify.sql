-- Verify: Check all branches and cross-branch query consistency
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_cross host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_cross');
USE t;

SELECT '=== CROSS-BRANCH QUERY VERIFICATION ===' as msg;

-- Sum verification (key metric)
SELECT 'Main total: ' || SUM(quantity)::VARCHAR || ' (expected: 425)' FROM inventory AT (BRANCH => 'main');
SELECT 'NYC_ops total: ' || SUM(quantity)::VARCHAR || ' (expected: 1025 = 425+50+50+500)' FROM inventory AT (BRANCH => 'nyc_ops');
SELECT 'LA_ops total: ' || SUM(quantity)::VARCHAR || ' (expected: 1675 = 425-50+1000+300)' FROM inventory AT (BRANCH => 'la_ops');
SELECT 'CHI_ops total: ' || SUM(quantity)::VARCHAR || ' (expected: 5350 = 425-75+5000)' FROM inventory AT (BRANCH => 'chi_ops');

SELECT '--- Main (unchanged, 4 items) ---' as msg;
SELECT * FROM inventory AT (BRANCH => 'main') ORDER BY id;

SELECT '--- NYC_ops (5 items, NYC quantities +50, added item 10) ---' as msg;
SELECT * FROM inventory AT (BRANCH => 'nyc_ops') ORDER BY id;

SELECT '--- LA_ops (5 items, Gadget deleted, added 20+21) ---' as msg;
SELECT * FROM inventory AT (BRANCH => 'la_ops') ORDER BY id;

SELECT '--- CHI_ops (5 items, Gizmo qty=0, added item 30) ---' as msg;
SELECT * FROM inventory AT (BRANCH => 'chi_ops') ORDER BY id;

SELECT '=== ISOLATION CHECKS ===' as msg;
SELECT 'Main still has Gadget (id=2): ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END
    FROM inventory AT (BRANCH => 'main') WHERE id = 2;
SELECT 'LA deleted Gadget (id=2): ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO' END
    FROM inventory AT (BRANCH => 'la_ops') WHERE id = 2;
SELECT 'NYC increased Widget qty: ' || CASE WHEN quantity = 150 THEN 'YES' ELSE 'NO' END
    FROM inventory AT (BRANCH => 'nyc_ops') WHERE id = 1;
SELECT 'Main Widget qty unchanged: ' || CASE WHEN quantity = 100 THEN 'YES' ELSE 'NO' END
    FROM inventory AT (BRANCH => 'main') WHERE id = 1;
