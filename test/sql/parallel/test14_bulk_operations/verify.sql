-- Verify: Bulk operation isolation
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_bulk host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_bulk');
USE t;

SELECT '=== BULK OPERATIONS VERIFICATION ===' as msg;

-- Main: 100 rows, sum=50500 (1+2+...+100)*10
SELECT 'Main: ' || COUNT(*)::VARCHAR || ' rows, sum=' || SUM(value)::VARCHAR || ' (expected: 100, 50500)' FROM data AT (BRANCH => 'main');

-- Bulk_insert: 200 rows (doubled), sum=151500 (50500 + 101000)
SELECT 'Bulk_insert: ' || COUNT(*)::VARCHAR || ' rows, sum=' || SUM(value)::VARCHAR || ' (expected: 200, 151500)' FROM data AT (BRANCH => 'bulk_insert');

-- Bulk_delete: 50 rows, sum=25500 (even numbers only)
SELECT 'Bulk_delete: ' || COUNT(*)::VARCHAR || ' rows, sum=' || SUM(value)::VARCHAR || ' (expected: 50, 25500)' FROM data AT (BRANCH => 'bulk_delete');

-- Bulk_update: 100 rows, sum=151500 (50500*3)
SELECT 'Bulk_update: ' || COUNT(*)::VARCHAR || ' rows, sum=' || SUM(value)::VARCHAR || ' (expected: 100, 151500)' FROM data AT (BRANCH => 'bulk_update');

-- Bulk_mixed: variable based on operations
SELECT 'Bulk_mixed: ' || COUNT(*)::VARCHAR || ' rows, sum=' || SUM(value)::VARCHAR FROM data AT (BRANCH => 'bulk_mixed');

SELECT '=== ISOLATION CHECKS ===' as msg;
SELECT 'Main unchanged (100 rows): ' || CASE WHEN COUNT(*) = 100 THEN 'YES' ELSE 'NO - ' || COUNT(*)::VARCHAR END FROM data AT (BRANCH => 'main');
SELECT 'Main sum unchanged (50500): ' || CASE WHEN SUM(value) = 50500 THEN 'YES' ELSE 'NO - ' || SUM(value)::VARCHAR END FROM data AT (BRANCH => 'main');
SELECT 'Bulk_insert doubled (200 rows): ' || CASE WHEN COUNT(*) = 200 THEN 'YES' ELSE 'NO' END FROM data AT (BRANCH => 'bulk_insert');
SELECT 'Bulk_delete halved (50 rows): ' || CASE WHEN COUNT(*) = 50 THEN 'YES' ELSE 'NO' END FROM data AT (BRANCH => 'bulk_delete');
SELECT 'Bulk_update all processed: ' || CASE WHEN COUNT(*) = 100 THEN 'YES' ELSE 'NO' END FROM data AT (BRANCH => 'bulk_update') WHERE status = 'processed';
SELECT 'Bulk_mixed has SUMMARY: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END FROM data AT (BRANCH => 'bulk_mixed') WHERE category = 'SUMMARY';
SELECT 'Main has NO SUMMARY: ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO' END FROM data AT (BRANCH => 'main') WHERE category = 'SUMMARY';
