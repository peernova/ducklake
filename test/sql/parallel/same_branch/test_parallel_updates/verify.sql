-- Verify: Each row should have value=50 (5 updates of +10 each)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=same_branch_updates host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/same_branch_updates');
USE t;

SELECT '=== SAME BRANCH PARALLEL UPDATE VERIFICATION ===' as msg;

SELECT * FROM counter ORDER BY id;

SELECT '=== SUMMARY ===' as msg;
SELECT CASE WHEN value = 50 THEN 'PASS' ELSE 'FAIL' END || ': Row ' || id::VARCHAR || ' = ' || value::VARCHAR || ' (expected: 50)' FROM counter ORDER BY id;
