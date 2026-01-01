-- Verify: All branches isolated despite cross-reads
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_cross_reads host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_cross_reads');
USE t;

SELECT '=== CROSS-BRANCH READS VERIFICATION ===' as msg;

SELECT 'Main: ' || COUNT(*)::VARCHAR || ' rows, sum=' || SUM(value)::VARCHAR || ' (expected: 3, 600)' FROM metrics AT (BRANCH => 'main');
SELECT 'Writer_A: ' || COUNT(*)::VARCHAR || ' rows (expected: 6 = 3 original + 3 inserted)' FROM metrics AT (BRANCH => 'writer_a');
SELECT 'Writer_B: ' || COUNT(*)::VARCHAR || ' rows (expected: 5 = 3 - 1 deleted + 3 inserted)' FROM metrics AT (BRANCH => 'writer_b');
SELECT 'Reader_only: ' || COUNT(*)::VARCHAR || ' rows (expected: 4 = 3 + 1)' FROM metrics AT (BRANCH => 'reader_only');

SELECT '--- Main (UNCHANGED) ---' as msg;
SELECT * FROM metrics AT (BRANCH => 'main') ORDER BY id;

SELECT '--- Writer_A (3 new rows + updates) ---' as msg;
SELECT * FROM metrics AT (BRANCH => 'writer_a') ORDER BY id;

SELECT '--- Writer_B (1 deleted + 3 new) ---' as msg;
SELECT * FROM metrics AT (BRANCH => 'writer_b') ORDER BY id;

SELECT '--- Reader_only (1 new row) ---' as msg;
SELECT * FROM metrics AT (BRANCH => 'reader_only') ORDER BY id;

SELECT '=== ISOLATION CHECKS ===' as msg;
SELECT 'Main unchanged (sum=600): ' || CASE WHEN SUM(value) = 600 THEN 'YES' ELSE 'NO' END FROM metrics AT (BRANCH => 'main');
SELECT 'Writer_A has 101-103: ' || CASE WHEN COUNT(*) = 3 THEN 'YES' ELSE 'NO' END FROM metrics AT (BRANCH => 'writer_a') WHERE id BETWEEN 101 AND 103;
SELECT 'Writer_B has 201-203: ' || CASE WHEN COUNT(*) = 3 THEN 'YES' ELSE 'NO' END FROM metrics AT (BRANCH => 'writer_b') WHERE id BETWEEN 201 AND 203;
SELECT 'Writer_B deleted id=2: ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO' END FROM metrics AT (BRANCH => 'writer_b') WHERE id = 2;
SELECT 'Main still has id=2: ' || CASE WHEN COUNT(*) = 1 THEN 'YES' ELSE 'NO' END FROM metrics AT (BRANCH => 'main') WHERE id = 2;
