-- Verify: Each branch should have its row updated to 150 (100 + 50)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=race_update_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/race_update_test');
USE t;

SELECT '=== UPDATE RACE CONDITION VERIFICATION ===' as msg;

-- Main should still have original values
SELECT 'Main - all rows should be 100:' as msg;
SELECT * FROM counter AT (BRANCH => 'main') ORDER BY id;

-- Branch A: row 1 should be 150
SELECT 'Branch A - row 1 should be 150:' as msg;
SELECT * FROM counter AT (BRANCH => 'branch_a') WHERE id = 1;
SELECT 'Branch A row 1 value: ' || value::VARCHAR || ' (expected: 150)' FROM counter AT (BRANCH => 'branch_a') WHERE id = 1;

-- Branch B: row 2 should be 150
SELECT 'Branch B - row 2 should be 150:' as msg;
SELECT * FROM counter AT (BRANCH => 'branch_b') WHERE id = 2;
SELECT 'Branch B row 2 value: ' || value::VARCHAR || ' (expected: 150)' FROM counter AT (BRANCH => 'branch_b') WHERE id = 2;

-- Branch C: row 3 should be 150
SELECT 'Branch C - row 3 should be 150:' as msg;
SELECT * FROM counter AT (BRANCH => 'branch_c') WHERE id = 3;
SELECT 'Branch C row 3 value: ' || value::VARCHAR || ' (expected: 150)' FROM counter AT (BRANCH => 'branch_c') WHERE id = 3;

-- Branch D: row 4 should be 150
SELECT 'Branch D - row 4 should be 150:' as msg;
SELECT * FROM counter AT (BRANCH => 'branch_d') WHERE id = 4;
SELECT 'Branch D row 4 value: ' || value::VARCHAR || ' (expected: 150)' FROM counter AT (BRANCH => 'branch_d') WHERE id = 4;

SELECT '=== SUMMARY ===' as msg;
SELECT
    CASE WHEN value = 150 THEN 'PASS' ELSE 'FAIL' END || ': Branch A row 1 = ' || value::VARCHAR
FROM counter AT (BRANCH => 'branch_a') WHERE id = 1;
SELECT
    CASE WHEN value = 150 THEN 'PASS' ELSE 'FAIL' END || ': Branch B row 2 = ' || value::VARCHAR
FROM counter AT (BRANCH => 'branch_b') WHERE id = 2;
SELECT
    CASE WHEN value = 150 THEN 'PASS' ELSE 'FAIL' END || ': Branch C row 3 = ' || value::VARCHAR
FROM counter AT (BRANCH => 'branch_c') WHERE id = 3;
SELECT
    CASE WHEN value = 150 THEN 'PASS' ELSE 'FAIL' END || ': Branch D row 4 = ' || value::VARCHAR
FROM counter AT (BRANCH => 'branch_d') WHERE id = 4;

-- Metadata check
SELECT '=== METADATA CHECK ===' as msg;
ATTACH 'postgres:dbname=race_update_test host=localhost port=5433 user=postgres password=postgres' AS pg;

SELECT 'Data files per branch:';
SELECT branch_id, COUNT(*) as file_count FROM pg.ducklake_data_file WHERE table_id = 1 GROUP BY branch_id ORDER BY branch_id;

SELECT 'Delete files per branch:';
SELECT branch_id, COUNT(*) as delete_count FROM pg.ducklake_delete_file WHERE table_id = 1 GROUP BY branch_id ORDER BY branch_id;

SELECT 'Snapshots per branch:';
SELECT branch_id, COUNT(*) as snapshot_count, MAX(snapshot_id) as max_snapshot_id
FROM pg.ducklake_snapshot GROUP BY branch_id ORDER BY branch_id;
