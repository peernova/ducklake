-- Verify: Edge-case partition handling isolation
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_null_part host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_null_part');
USE t;

SELECT '=== EDGE-CASE PARTITION VERIFICATION ===' as msg;

SELECT 'Main: ' || COUNT(*)::VARCHAR || ' rows, uncategorized=' || (SELECT COUNT(*) FROM logs AT (BRANCH => 'main') WHERE category = 'uncategorized')::VARCHAR || ' (expected: 8, 3)' FROM logs AT (BRANCH => 'main');
SELECT 'Handle_nulls: ' || COUNT(*)::VARCHAR || ' rows, uncategorized=' || (SELECT COUNT(*) FROM logs AT (BRANCH => 'handle_nulls') WHERE category = 'uncategorized')::VARCHAR || ' (expected: 8, 0)' FROM logs AT (BRANCH => 'handle_nulls');
SELECT 'Delete_nulls: ' || COUNT(*)::VARCHAR || ' rows, uncategorized=' || (SELECT COUNT(*) FROM logs AT (BRANCH => 'delete_nulls') WHERE category = 'uncategorized')::VARCHAR || ' (expected: 5, 0)' FROM logs AT (BRANCH => 'delete_nulls');
SELECT 'Keep_nulls: ' || COUNT(*)::VARCHAR || ' rows, uncategorized=' || (SELECT COUNT(*) FROM logs AT (BRANCH => 'keep_nulls') WHERE category = 'uncategorized')::VARCHAR || ' (expected: 11, 6)' FROM logs AT (BRANCH => 'keep_nulls');

SELECT '--- Main (original uncategorized) ---' as msg;
SELECT category, COUNT(*) as cnt FROM logs AT (BRANCH => 'main') GROUP BY category ORDER BY category;

SELECT '--- Handle_nulls (no uncategorized) ---' as msg;
SELECT category, COUNT(*) as cnt FROM logs AT (BRANCH => 'handle_nulls') GROUP BY category ORDER BY category;

SELECT '--- Delete_nulls (uncategorized removed) ---' as msg;
SELECT category, COUNT(*) as cnt FROM logs AT (BRANCH => 'delete_nulls') GROUP BY category ORDER BY category;

SELECT '--- Keep_nulls (more uncategorized) ---' as msg;
SELECT category, COUNT(*) as cnt FROM logs AT (BRANCH => 'keep_nulls') GROUP BY category ORDER BY category;

SELECT '=== ISOLATION CHECKS ===' as msg;
SELECT 'Main still has 3 uncategorized: ' || CASE WHEN COUNT(*) = 3 THEN 'YES' ELSE 'NO' END FROM logs AT (BRANCH => 'main') WHERE category = 'uncategorized';
SELECT 'Handle_nulls has 0 uncategorized: ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO' END FROM logs AT (BRANCH => 'handle_nulls') WHERE category = 'uncategorized';
SELECT 'Delete_nulls removed uncategorized: ' || CASE WHEN COUNT(*) = 5 THEN 'YES (5 rows)' ELSE 'NO' END FROM logs AT (BRANCH => 'delete_nulls');
SELECT 'Keep_nulls has 6 uncategorized: ' || CASE WHEN COUNT(*) = 6 THEN 'YES' ELSE 'NO - ' || COUNT(*)::VARCHAR END FROM logs AT (BRANCH => 'keep_nulls') WHERE category = 'uncategorized';
