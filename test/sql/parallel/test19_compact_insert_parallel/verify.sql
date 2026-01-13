-- Verify: Check that all data is intact after parallel compaction and inserts on same branch
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=parallel_compact host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_compact');
USE t;

SELECT '=== VERIFICATION ===' as msg;

-- Check main branch - should still have original 2000 rows
CALL ducklake_use_branch('t', 'main');
SELECT 'Main branch count: ' || COUNT(*) as msg FROM orders;
SELECT 'Main expected: 2000' as msg;

-- Verify main has only original regions
SELECT 'Main regions: ' || string_agg(DISTINCT region, ', ' ORDER BY region) as msg FROM orders;

-- Check test_branch - should have:
-- Original 2000 (from main) + 1000 (branch initial) + 1000 (concurrent inserts) = 4000
CALL ducklake_use_branch('t', 'test_branch');
SELECT 'test_branch count: ' || COUNT(*) as msg FROM orders;
SELECT 'test_branch expected: 4000' as msg;

-- Verify test_branch has all regions
SELECT 'test_branch regions: ' || string_agg(DISTINCT region, ', ' ORDER BY region) as msg FROM orders;

-- Verify concurrent inserts are present
SELECT 'Concurrent insert rows (LATAM+AFRICA from 3001-4000): ' || COUNT(*) as msg
FROM orders WHERE customer LIKE 'concurrent%';
SELECT 'Expected concurrent rows: 1000' as msg;

-- Verify data integrity - check all IDs are present
SELECT 'ID range check - min: ' || MIN(id) || ', max: ' || MAX(id) as msg FROM orders;
SELECT 'Expected: min=1, max=4000' as msg;

-- Check for any duplicates
SELECT 'Duplicate IDs: ' || COUNT(*) as msg
FROM (SELECT id FROM orders GROUP BY id HAVING COUNT(*) > 1);
SELECT 'Expected duplicates: 0' as msg;

SELECT '=== VERIFICATION COMPLETE ===' as msg;
