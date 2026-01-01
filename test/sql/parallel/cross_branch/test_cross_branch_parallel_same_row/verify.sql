-- Verify: Each branch should have its unique value for the SAME row
-- Main: 0, Branch A: 100, Branch B: 200, Branch C: 300, Branch D: 400
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=race_same_row host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/race_same_row');
USE t;

SELECT '=== SAME ROW RACE CONDITION VERIFICATION ===' as msg;
SELECT 'All branches update the SAME row (id=1) with different values' as msg;

SELECT 'Main value: ' || value::VARCHAR || ' (expected: 0)' FROM counter AT (BRANCH => 'main') WHERE id = 1;
SELECT 'Branch A value: ' || value::VARCHAR || ' (expected: 100)' FROM counter AT (BRANCH => 'branch_a') WHERE id = 1;
SELECT 'Branch B value: ' || value::VARCHAR || ' (expected: 200)' FROM counter AT (BRANCH => 'branch_b') WHERE id = 1;
SELECT 'Branch C value: ' || value::VARCHAR || ' (expected: 300)' FROM counter AT (BRANCH => 'branch_c') WHERE id = 1;
SELECT 'Branch D value: ' || value::VARCHAR || ' (expected: 400)' FROM counter AT (BRANCH => 'branch_d') WHERE id = 1;

SELECT '=== SUMMARY ===' as msg;
SELECT CASE WHEN value = 0 THEN 'PASS' ELSE 'FAIL' END || ': Main = ' || value::VARCHAR FROM counter AT (BRANCH => 'main') WHERE id = 1;
SELECT CASE WHEN value = 100 THEN 'PASS' ELSE 'FAIL' END || ': Branch A = ' || value::VARCHAR FROM counter AT (BRANCH => 'branch_a') WHERE id = 1;
SELECT CASE WHEN value = 200 THEN 'PASS' ELSE 'FAIL' END || ': Branch B = ' || value::VARCHAR FROM counter AT (BRANCH => 'branch_b') WHERE id = 1;
SELECT CASE WHEN value = 300 THEN 'PASS' ELSE 'FAIL' END || ': Branch C = ' || value::VARCHAR FROM counter AT (BRANCH => 'branch_c') WHERE id = 1;
SELECT CASE WHEN value = 400 THEN 'PASS' ELSE 'FAIL' END || ': Branch D = ' || value::VARCHAR FROM counter AT (BRANCH => 'branch_d') WHERE id = 1;
