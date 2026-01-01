-- Verify: Each branch should have exactly 6 rows (1 from main + 5 inserts)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=race_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/race_test');
USE t;

SELECT '=== RACE CONDITION VERIFICATION ===' as msg;

-- Each branch should have 6 rows: 1 (main) + 5 (inserts)
SELECT 'Main: ' || COUNT(*)::VARCHAR || ' rows (expected: 1)' FROM counter AT (BRANCH => 'main');
SELECT 'Branch A: ' || COUNT(*)::VARCHAR || ' rows (expected: 6)' FROM counter AT (BRANCH => 'branch_a');
SELECT 'Branch B: ' || COUNT(*)::VARCHAR || ' rows (expected: 6)' FROM counter AT (BRANCH => 'branch_b');
SELECT 'Branch C: ' || COUNT(*)::VARCHAR || ' rows (expected: 6)' FROM counter AT (BRANCH => 'branch_c');
SELECT 'Branch D: ' || COUNT(*)::VARCHAR || ' rows (expected: 6)' FROM counter AT (BRANCH => 'branch_d');

SELECT '=== DETAILED DATA ===' as msg;

SELECT '--- Main ---' as msg;
SELECT * FROM counter AT (BRANCH => 'main') ORDER BY id;

SELECT '--- Branch A ---' as msg;
SELECT * FROM counter AT (BRANCH => 'branch_a') ORDER BY id;

SELECT '--- Branch B ---' as msg;
SELECT * FROM counter AT (BRANCH => 'branch_b') ORDER BY id;

SELECT '--- Branch C ---' as msg;
SELECT * FROM counter AT (BRANCH => 'branch_c') ORDER BY id;

SELECT '--- Branch D ---' as msg;
SELECT * FROM counter AT (BRANCH => 'branch_d') ORDER BY id;

-- Check metadata consistency
SELECT '=== METADATA CHECK ===' as msg;

ATTACH 'postgres:dbname=race_test host=localhost port=5433 user=postgres password=postgres' AS pg;

SELECT 'Data files per branch:';
SELECT branch_id, COUNT(*) as file_count FROM pg.ducklake_data_file WHERE table_id = 1 GROUP BY branch_id ORDER BY branch_id;

SELECT 'Snapshots per branch:';
SELECT branch_id, COUNT(*) as snapshot_count, MAX(next_file_id) as max_next_file_id
FROM pg.ducklake_snapshot GROUP BY branch_id ORDER BY branch_id;
