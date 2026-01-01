-- Verify partition deletion works correctly across branches
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=delete_partition_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/delete_partition_test');
USE t;

SELECT '=== DELETE BY PARTITION VALUE VERIFICATION ===' as msg;

-- Check counts per severity on main
SELECT '--- Main Branch (all logs) ---' as msg;
SELECT severity, COUNT(*) as count FROM logs AT (BRANCH => 'main') GROUP BY severity ORDER BY severity;

-- Check counts on no_info branch
SELECT '--- no_info Branch (INFO deleted) ---' as msg;
SELECT severity, COUNT(*) as count FROM logs AT (BRANCH => 'no_info') GROUP BY severity ORDER BY severity;

-- Check counts on clean_logs branch
SELECT '--- clean_logs Branch (ERROR+WARNING deleted) ---' as msg;
SELECT severity, COUNT(*) as count FROM logs AT (BRANCH => 'clean_logs') GROUP BY severity ORDER BY severity;

-- Check counts on critical_only branch
SELECT '--- critical_only Branch (only CRITICAL) ---' as msg;
SELECT severity, COUNT(*) as count FROM logs AT (BRANCH => 'critical_only') GROUP BY severity ORDER BY severity;

SELECT '=== SUMMARY ===' as msg;

-- Verify main has 12 total logs
SELECT CASE WHEN COUNT(*) = 12 THEN 'PASS' ELSE 'FAIL' END || ': main has 12 logs = ' || COUNT(*)::VARCHAR
FROM logs AT (BRANCH => 'main');

-- Verify no_info has 7 logs (12 - 5 INFO)
SELECT CASE WHEN COUNT(*) = 7 THEN 'PASS' ELSE 'FAIL' END || ': no_info has 7 logs = ' || COUNT(*)::VARCHAR
FROM logs AT (BRANCH => 'no_info');

-- Verify no_info has 0 INFO logs
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END || ': no_info has 0 INFO logs = ' || COUNT(*)::VARCHAR
FROM logs AT (BRANCH => 'no_info') WHERE severity = 'INFO';

-- Verify clean_logs has 6 logs (12 - 3 WARNING - 3 ERROR)
SELECT CASE WHEN COUNT(*) = 6 THEN 'PASS' ELSE 'FAIL' END || ': clean_logs has 6 logs = ' || COUNT(*)::VARCHAR
FROM logs AT (BRANCH => 'clean_logs');

-- Verify clean_logs has 0 ERROR logs
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END || ': clean_logs has 0 ERROR logs = ' || COUNT(*)::VARCHAR
FROM logs AT (BRANCH => 'clean_logs') WHERE severity = 'ERROR';

-- Verify clean_logs has 0 WARNING logs
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END || ': clean_logs has 0 WARNING logs = ' || COUNT(*)::VARCHAR
FROM logs AT (BRANCH => 'clean_logs') WHERE severity = 'WARNING';

-- Verify critical_only has 1 log
SELECT CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END || ': critical_only has 1 log = ' || COUNT(*)::VARCHAR
FROM logs AT (BRANCH => 'critical_only');

-- Verify critical_only has only CRITICAL logs
SELECT CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END || ': critical_only has 1 CRITICAL log = ' || COUNT(*)::VARCHAR
FROM logs AT (BRANCH => 'critical_only') WHERE severity = 'CRITICAL';

-- Verify main is unchanged (still has all severities)
SELECT CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END || ': main still has 5 INFO logs = ' || COUNT(*)::VARCHAR
FROM logs AT (BRANCH => 'main') WHERE severity = 'INFO';

SELECT CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END || ': main still has 3 ERROR logs = ' || COUNT(*)::VARCHAR
FROM logs AT (BRANCH => 'main') WHERE severity = 'ERROR';

SELECT CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END || ': main still has 3 WARNING logs = ' || COUNT(*)::VARCHAR
FROM logs AT (BRANCH => 'main') WHERE severity = 'WARNING';

SELECT CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END || ': main still has 1 CRITICAL log = ' || COUNT(*)::VARCHAR
FROM logs AT (BRANCH => 'main') WHERE severity = 'CRITICAL';

SELECT '=== ALL TESTS COMPLETE ===' as msg;
