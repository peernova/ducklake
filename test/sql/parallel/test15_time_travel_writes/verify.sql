-- Verify: Time-travel consistency
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_timetravel host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_timetravel');
USE t;

SELECT '=== TIME-TRAVEL VERIFICATION ===' as msg;

SELECT 'Main: ' || COUNT(*)::VARCHAR || ' rows (expected: 3)' FROM history AT (BRANCH => 'main');
SELECT 'Writer_fast: ' || COUNT(*)::VARCHAR || ' rows (expected: 8 = 3+5)' FROM history AT (BRANCH => 'writer_fast');
SELECT 'Writer_slow: ' || COUNT(*)::VARCHAR || ' rows (expected: 3 = 3-1+1)' FROM history AT (BRANCH => 'writer_slow');
SELECT 'Reader_history: ' || COUNT(*)::VARCHAR || ' rows (expected: 4 = 3+1)' FROM history AT (BRANCH => 'reader_history');
SELECT 'Reader_branch: ' || COUNT(*)::VARCHAR || ' rows (expected: 5 = 3+2)' FROM history AT (BRANCH => 'reader_branch');

SELECT '--- Main (original 3 rows) ---' as msg;
SELECT * FROM history AT (BRANCH => 'main') ORDER BY id;

SELECT '--- Writer_fast (5 rapid inserts) ---' as msg;
SELECT * FROM history AT (BRANCH => 'writer_fast') ORDER BY id;

SELECT '--- Writer_slow (updates/deletes) ---' as msg;
SELECT * FROM history AT (BRANCH => 'writer_slow') ORDER BY id;

SELECT '=== ISOLATION CHECKS ===' as msg;
SELECT 'Main unchanged (3 rows): ' || CASE WHEN COUNT(*) = 3 THEN 'YES' ELSE 'NO' END FROM history AT (BRANCH => 'main');
SELECT 'Main has id=2: ' || CASE WHEN COUNT(*) = 1 THEN 'YES' ELSE 'NO' END FROM history AT (BRANCH => 'main') WHERE id = 2;
SELECT 'Writer_slow deleted id=2: ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO' END FROM history AT (BRANCH => 'writer_slow') WHERE id = 2;
SELECT 'Writer_fast has 101-105: ' || CASE WHEN COUNT(*) = 5 THEN 'YES' ELSE 'NO' END FROM history AT (BRANCH => 'writer_fast') WHERE id BETWEEN 101 AND 105;
SELECT 'Main has NO 101-105: ' || CASE WHEN COUNT(*) = 0 THEN 'YES - isolated' ELSE 'NO - LEAK!' END FROM history AT (BRANCH => 'main') WHERE id BETWEEN 101 AND 105;

SELECT '=== BRANCH SNAPSHOT OVERVIEW ===' as msg;
SELECT branch_name, head_snapshot_id FROM ducklake_branches('t') ORDER BY branch_id;
