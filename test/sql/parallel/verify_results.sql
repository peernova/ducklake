-- Verify: Run after all workers complete to check isolation
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_data');
USE t;

SELECT '=== VERIFICATION ===' as msg;

SELECT 'Main: ' || COUNT(*)::VARCHAR || ' users (expected: 3)' FROM users AT (BRANCH => 'main');
SELECT 'Branch A: ' || COUNT(*)::VARCHAR || ' users (expected: 5)' FROM users AT (BRANCH => 'branch_a');
SELECT 'Branch B: ' || COUNT(*)::VARCHAR || ' users (expected: 4)' FROM users AT (BRANCH => 'branch_b');
SELECT 'Branch C: ' || COUNT(*)::VARCHAR || ' users (expected: 4)' FROM users AT (BRANCH => 'branch_c');

SELECT '--- Main (unchanged) ---' as msg;
SELECT * FROM users AT (BRANCH => 'main') ORDER BY id;

SELECT '--- Branch A (users 10,11 added, user 1 premium) ---' as msg;
SELECT * FROM users AT (BRANCH => 'branch_a') ORDER BY id;

SELECT '--- Branch B (user 2 deleted, users 20,21 added) ---' as msg;
SELECT * FROM users AT (BRANCH => 'branch_b') ORDER BY id;

SELECT '--- Branch C (email column, user 30 added) ---' as msg;
SELECT * FROM users AT (BRANCH => 'branch_c') ORDER BY id;
