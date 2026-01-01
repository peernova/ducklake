-- Verify: Only category='Z' should remain (5 rows)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=same_branch_deletes host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/same_branch_deletes');
USE t;

SELECT '=== SAME BRANCH PARALLEL DELETE VERIFICATION ===' as msg;

SELECT category, COUNT(*) as cnt FROM data GROUP BY category ORDER BY category;
SELECT 'Total rows: ' || COUNT(*)::VARCHAR || ' (expected: 5)' FROM data;

SELECT '=== SUMMARY ===' as msg;
SELECT CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END || ': Total rows = ' || COUNT(*)::VARCHAR FROM data;
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END || ': Category A deleted = ' || COUNT(*)::VARCHAR FROM data WHERE category = 'A';
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END || ': Category B deleted = ' || COUNT(*)::VARCHAR FROM data WHERE category = 'B';
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END || ': Category C deleted = ' || COUNT(*)::VARCHAR FROM data WHERE category = 'C';
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END || ': Category D deleted = ' || COUNT(*)::VARCHAR FROM data WHERE category = 'D';
SELECT CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END || ': Category Z kept = ' || COUNT(*)::VARCHAR FROM data WHERE category = 'Z';
