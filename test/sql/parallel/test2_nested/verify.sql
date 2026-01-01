-- Verify: Check all branches after parallel operations
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_nested host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_nested');
USE t;

SELECT '=== NESTED BRANCHES VERIFICATION ===' as msg;

-- Count verification
SELECT 'Main: ' || COUNT(*)::VARCHAR || ' products (expected: 4)' FROM products AT (BRANCH => 'main');
SELECT 'Dev: ' || COUNT(*)::VARCHAR || ' products (expected: 6)' FROM products AT (BRANCH => 'dev');
SELECT 'Feature_A: ' || COUNT(*)::VARCHAR || ' products (expected: 7)' FROM products AT (BRANCH => 'feature_a');
SELECT 'Feature_B: ' || COUNT(*)::VARCHAR || ' products (expected: 6)' FROM products AT (BRANCH => 'feature_b');
SELECT 'Hotfix: ' || COUNT(*)::VARCHAR || ' products (expected: 7)' FROM products AT (BRANCH => 'feature_a_hotfix');

SELECT '--- Main (original 4 products) ---' as msg;
SELECT * FROM products AT (BRANCH => 'main') ORDER BY id;

SELECT '--- Dev (5 from before + DevProduct=100) ---' as msg;
SELECT * FROM products AT (BRANCH => 'dev') ORDER BY id;

SELECT '--- Feature_A (6 - Desk deleted + 200,201 added) ---' as msg;
SELECT * FROM products AT (BRANCH => 'feature_a') ORDER BY id;

SELECT '--- Feature_B (5 + 300 added, premium- prefix) ---' as msg;
SELECT * FROM products AT (BRANCH => 'feature_b') ORDER BY id;

SELECT '--- Hotfix (grandchild: 6 from feature_a + 999, Monitor price fixed) ---' as msg;
SELECT * FROM products AT (BRANCH => 'feature_a_hotfix') ORDER BY id;

-- Specific checks
SELECT '=== ISOLATION CHECKS ===' as msg;
SELECT 'Main has Desk (id=3): ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END FROM products AT (BRANCH => 'main') WHERE id = 3;
SELECT 'Feature_A has Desk (id=3): ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END FROM products AT (BRANCH => 'feature_a') WHERE id = 3;
-- Hotfix was created BEFORE feature_a deleted Desk, so it still has Desk
SELECT 'Hotfix has Desk (branched before delete): ' || CASE WHEN COUNT(*) > 0 THEN 'YES - correct' ELSE 'NO - wrong' END FROM products AT (BRANCH => 'feature_a_hotfix') WHERE id = 3;
SELECT 'Dev has DevProduct (100): ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END FROM products AT (BRANCH => 'dev') WHERE id = 100;
SELECT 'Main has DevProduct (100): ' || CASE WHEN COUNT(*) > 0 THEN 'YES - LEAK!' ELSE 'NO - isolated' END FROM products AT (BRANCH => 'main') WHERE id = 100;
