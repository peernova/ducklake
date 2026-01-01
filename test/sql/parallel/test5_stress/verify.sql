-- Verify: Check all 6 branches after stress test
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_stress host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_stress');
USE t;

SELECT '=== STRESS TEST VERIFICATION (6 Workers) ===' as msg;

-- Count and sum for each branch
SELECT 'Main: count=' || COUNT(*)::VARCHAR || ', sum=' || SUM(amount)::VARCHAR || ' (expected: 5, 1950)' FROM transactions AT (BRANCH => 'main');
SELECT 'W1: count=' || COUNT(*)::VARCHAR || ', sum=' || SUM(amount)::VARCHAR || ' (expected: 10, 3450)' FROM transactions AT (BRANCH => 'worker_1');
SELECT 'W2: count=' || COUNT(*)::VARCHAR || ', sum=' || SUM(amount)::VARCHAR || ' (expected: 3, 9699)' FROM transactions AT (BRANCH => 'worker_2');
SELECT 'W3: count=' || COUNT(*)::VARCHAR || ', sum=' || SUM(amount)::VARCHAR || ' (expected: 5, 4200)' FROM transactions AT (BRANCH => 'worker_3');
SELECT 'W4: count=' || COUNT(*)::VARCHAR || ', sum=' || SUM(amount)::VARCHAR || ' (expected: 6, 2727)' FROM transactions AT (BRANCH => 'worker_4');
SELECT 'W5: count=' || COUNT(*)::VARCHAR || ', sum=' || SUM(amount)::VARCHAR || ' (expected: 6, 2483)' FROM transactions AT (BRANCH => 'worker_5');
SELECT 'W6: count=' || COUNT(*)::VARCHAR || ', sum=' || SUM(amount)::VARCHAR || ' (expected: 13, 2800)' FROM transactions AT (BRANCH => 'worker_6');

SELECT '--- Main (original 5 transactions) ---' as msg;
SELECT * FROM transactions AT (BRANCH => 'main') ORDER BY id;

SELECT '--- W1 (5 heavy inserts) ---' as msg;
SELECT * FROM transactions AT (BRANCH => 'worker_1') ORDER BY id;

SELECT '--- W2 (3 deletes + 1 bonus) ---' as msg;
SELECT * FROM transactions AT (BRANCH => 'worker_2') ORDER BY id;

SELECT '--- W3 (doubled deposits, prefixed accounts) ---' as msg;
SELECT * FROM transactions AT (BRANCH => 'worker_3') ORDER BY id;

SELECT '--- W4 (schema change + new cols) ---' as msg;
SELECT * FROM transactions AT (BRANCH => 'worker_4') ORDER BY id;

SELECT '--- W5 (mixed ops + cross queries) ---' as msg;
SELECT * FROM transactions AT (BRANCH => 'worker_5') ORDER BY id;

SELECT '--- W6 (bulk delete + 10 inserts) ---' as msg;
SELECT * FROM transactions AT (BRANCH => 'worker_6') ORDER BY id;

SELECT '=== ISOLATION CHECKS ===' as msg;
SELECT 'Main unchanged: ' || CASE WHEN COUNT(*) = 5 AND SUM(amount) = 1950 THEN 'YES' ELSE 'NO' END FROM transactions AT (BRANCH => 'main');
SELECT 'W1 has 5 extra: ' || CASE WHEN COUNT(*) = 10 THEN 'YES' ELSE 'NO' END FROM transactions AT (BRANCH => 'worker_1');
SELECT 'W2 deleted ids 1-3: ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO' END FROM transactions AT (BRANCH => 'worker_2') WHERE id IN (1,2,3);
SELECT 'W3 doubled deposits: ' || CASE WHEN SUM(amount) = 4200 THEN 'YES' ELSE 'NO' END FROM transactions AT (BRANCH => 'worker_3');
SELECT 'W6 no withdrawals: ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO' END FROM transactions AT (BRANCH => 'worker_6') WHERE type = 'withdrawal';
