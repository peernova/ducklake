-- Verify: Should have 21 rows (1 setup + 5*4 workers)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=same_branch_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/same_branch_test');
USE t;

SELECT '=== SAME BRANCH PARALLEL INSERT VERIFICATION ===' as msg;

SELECT 'Total rows: ' || COUNT(*)::VARCHAR || ' (expected: 21)' FROM counter;
SELECT 'Worker1 rows: ' || COUNT(*)::VARCHAR || ' (expected: 5)' FROM counter WHERE worker = 'worker1';
SELECT 'Worker2 rows: ' || COUNT(*)::VARCHAR || ' (expected: 5)' FROM counter WHERE worker = 'worker2';
SELECT 'Worker3 rows: ' || COUNT(*)::VARCHAR || ' (expected: 5)' FROM counter WHERE worker = 'worker3';
SELECT 'Worker4 rows: ' || COUNT(*)::VARCHAR || ' (expected: 5)' FROM counter WHERE worker = 'worker4';

SELECT '=== SUMMARY ===' as msg;
SELECT CASE WHEN COUNT(*) = 21 THEN 'PASS' ELSE 'FAIL' END || ': Total = ' || COUNT(*)::VARCHAR FROM counter;
SELECT CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END || ': Worker1 = ' || COUNT(*)::VARCHAR FROM counter WHERE worker = 'worker1';
SELECT CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END || ': Worker2 = ' || COUNT(*)::VARCHAR FROM counter WHERE worker = 'worker2';
SELECT CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END || ': Worker3 = ' || COUNT(*)::VARCHAR FROM counter WHERE worker = 'worker3';
SELECT CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END || ': Worker4 = ' || COUNT(*)::VARCHAR FROM counter WHERE worker = 'worker4';

-- Show snapshots
SELECT '=== SNAPSHOTS ===' as msg;
SELECT * FROM ducklake_snapshots('t');
