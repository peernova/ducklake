-- Verify: Each branch has its own independent version of the same rows
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_same_row host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_same_row');
USE t;

SELECT '=== SAME ROW CONFLICT VERIFICATION ===' as msg;

-- Main: UNCHANGED
SELECT 'Main: ' || COUNT(*)::VARCHAR || ' rows (expected: 5)' FROM accounts AT (BRANCH => 'main');

-- Branch A: 5 rows, row 1 has balance=5000, status=premium
SELECT 'Branch_A: ' || COUNT(*)::VARCHAR || ' rows (expected: 5)' FROM accounts AT (BRANCH => 'branch_a');

-- Branch B: 4 rows (row 1 deleted)
SELECT 'Branch_B: ' || COUNT(*)::VARCHAR || ' rows (expected: 4)' FROM accounts AT (BRANCH => 'branch_b');

-- Branch C: 5 rows, row 1 name=UPDATED_C
SELECT 'Branch_C: ' || COUNT(*)::VARCHAR || ' rows (expected: 5)' FROM accounts AT (BRANCH => 'branch_c');

-- Branch D: 5 rows, rows 1,2,3 have +100 balance
SELECT 'Branch_D: ' || COUNT(*)::VARCHAR || ' rows, sum=' || SUM(balance)::VARCHAR || ' (expected: 5, 15300)' FROM accounts AT (BRANCH => 'branch_d');

-- Branch E: 3 rows (rows 1,2 deleted, row 3 closed)
SELECT 'Branch_E: ' || COUNT(*)::VARCHAR || ' rows (expected: 3)' FROM accounts AT (BRANCH => 'branch_e');

SELECT '--- Main Branch (UNCHANGED) ---' as msg;
SELECT * FROM accounts AT (BRANCH => 'main') ORDER BY id;

SELECT '--- Branch A (row 1: balance=5000, status=premium) ---' as msg;
SELECT * FROM accounts AT (BRANCH => 'branch_a') WHERE id = 1;

SELECT '--- Branch B (row 1 DELETED) ---' as msg;
SELECT 'Row 1 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - ERROR!' ELSE 'NO - correct' END FROM accounts AT (BRANCH => 'branch_b') WHERE id = 1;

SELECT '--- Branch C (row 1 name=UPDATED_C) ---' as msg;
SELECT * FROM accounts AT (BRANCH => 'branch_c') WHERE id IN (1, 2);

SELECT '--- Branch D (rows 1,2,3 balance +100) ---' as msg;
SELECT id, name, balance FROM accounts AT (BRANCH => 'branch_d') WHERE id <= 3 ORDER BY id;

SELECT '--- Branch E (rows 1,2 deleted, row 3 closed) ---' as msg;
SELECT * FROM accounts AT (BRANCH => 'branch_e') ORDER BY id;

SELECT '=== ROW 1 STATE ACROSS ALL BRANCHES ===' as msg;
SELECT 'Main row 1: ' || name || ', balance=' || balance::VARCHAR || ', status=' || status FROM accounts AT (BRANCH => 'main') WHERE id = 1;
SELECT 'Branch_A row 1: ' || name || ', balance=' || balance::VARCHAR || ', status=' || status FROM accounts AT (BRANCH => 'branch_a') WHERE id = 1;
SELECT 'Branch_B row 1: DELETED' as msg WHERE NOT EXISTS (SELECT 1 FROM accounts AT (BRANCH => 'branch_b') WHERE id = 1);
SELECT 'Branch_C row 1: ' || name || ', balance=' || balance::VARCHAR || ', status=' || status FROM accounts AT (BRANCH => 'branch_c') WHERE id = 1;
SELECT 'Branch_D row 1: ' || name || ', balance=' || balance::VARCHAR || ', status=' || status FROM accounts AT (BRANCH => 'branch_d') WHERE id = 1;
SELECT 'Branch_E row 1: DELETED' as msg WHERE NOT EXISTS (SELECT 1 FROM accounts AT (BRANCH => 'branch_e') WHERE id = 1);

SELECT '=== ISOLATION CHECKS ===' as msg;
-- Main unchanged
SELECT 'Main row 1 unchanged (Alice, 1000): ' || CASE WHEN name = 'Alice' AND balance = 1000.00 THEN 'YES' ELSE 'NO - CORRUPTED!' END FROM accounts AT (BRANCH => 'main') WHERE id = 1;
-- Branch A has premium status
SELECT 'Branch_A row 1 premium: ' || CASE WHEN status = 'premium' THEN 'YES' ELSE 'NO' END FROM accounts AT (BRANCH => 'branch_a') WHERE id = 1;
-- Branch B deleted row 1
SELECT 'Branch_B row 1 deleted: ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO' END FROM accounts AT (BRANCH => 'branch_b') WHERE id = 1;
-- Branch C renamed row 1
SELECT 'Branch_C row 1 renamed: ' || CASE WHEN name = 'UPDATED_C' THEN 'YES' ELSE 'NO' END FROM accounts AT (BRANCH => 'branch_c') WHERE id = 1;
-- Branch D added 100
SELECT 'Branch_D row 1 balance 1100: ' || CASE WHEN balance = 1100.00 THEN 'YES' ELSE 'NO' END FROM accounts AT (BRANCH => 'branch_d') WHERE id = 1;
-- Branch E deleted row 1
SELECT 'Branch_E row 1 deleted: ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO' END FROM accounts AT (BRANCH => 'branch_e') WHERE id = 1;
