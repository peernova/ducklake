-- Verify: Check MERGE isolation across branches
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=parallel_merge host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_merge');
USE t;

SELECT '=== VERIFICATION: PARALLEL MERGE ISOLATION ===' as msg;

-- ============================================
-- MAIN BRANCH: Should be unchanged
-- ============================================
CALL ducklake_use_branch('t', 'main');
SELECT '--- MAIN BRANCH (should be unchanged) ---' as msg;

SELECT 'Main: product count: ' || COUNT(*) as check FROM products;
-- Should be 5 (original)

SELECT 'Main: product 1 name: ' || name as check FROM products WHERE product_id = 1;
-- Should be 'Widget A' (not updated)

SELECT 'Main: product 6 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - WRONG!' ELSE 'NO - correct' END as check
FROM products WHERE product_id = 6;
-- Should NOT exist

-- ============================================
-- BRANCH_A: Updated 1,2 + Inserted 6,7
-- ============================================
CALL ducklake_use_branch('t', 'branch_a');
SELECT '--- BRANCH_A (products 1,2 updated; 6,7 inserted) ---' as msg;

SELECT 'Branch_A: product count: ' || COUNT(*) as check FROM products;
-- Should be 7

SELECT 'Branch_A: product 1 name: ' || CASE WHEN name = 'Widget A Updated' THEN 'UPDATED - correct' ELSE 'NOT UPDATED - WRONG!' END as check
FROM products WHERE product_id = 1;

SELECT 'Branch_A: product 6 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - correct' ELSE 'NO - WRONG!' END as check
FROM products WHERE product_id = 6;

SELECT 'Branch_A: product 8 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - WRONG!' ELSE 'NO - correct' END as check
FROM products WHERE product_id = 8;
-- 8 was inserted on branch_b, not branch_a

-- ============================================
-- BRANCH_B: Updated 3,4 + Inserted 8,9
-- ============================================
CALL ducklake_use_branch('t', 'branch_b');
SELECT '--- BRANCH_B (products 3,4 updated; 8,9 inserted) ---' as msg;

SELECT 'Branch_B: product count: ' || COUNT(*) as check FROM products;
-- Should be 7

SELECT 'Branch_B: product 3 name: ' || CASE WHEN name = 'Widget C Modified' THEN 'UPDATED - correct' ELSE 'NOT UPDATED - WRONG!' END as check
FROM products WHERE product_id = 3;

SELECT 'Branch_B: product 8 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - correct' ELSE 'NO - WRONG!' END as check
FROM products WHERE product_id = 8;

SELECT 'Branch_B: product 1 name: ' || CASE WHEN name = 'Widget A' THEN 'ORIGINAL - correct' ELSE 'MODIFIED - WRONG!' END as check
FROM products WHERE product_id = 1;
-- 1 was updated on branch_a, should be original here

-- ============================================
-- BRANCH_C: Deleted 5 + Inserted 10,11
-- ============================================
CALL ducklake_use_branch('t', 'branch_c');
SELECT '--- BRANCH_C (product 5 deleted; 10,11 inserted) ---' as msg;

SELECT 'Branch_C: product count: ' || COUNT(*) as check FROM products;
-- Should be 6 (5 original - 1 deleted + 2 inserted)

SELECT 'Branch_C: product 5 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - WRONG!' ELSE 'NO (deleted) - correct' END as check
FROM products WHERE product_id = 5;

SELECT 'Branch_C: product 10 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - correct' ELSE 'NO - WRONG!' END as check
FROM products WHERE product_id = 10;

-- ============================================
-- TIME TRAVEL: Access deleted product from main
-- ============================================
SELECT '--- TIME TRAVEL ---' as msg;

SELECT 'From branch_c, access main product 5 via time travel:' as msg;
SELECT * FROM products AT (BRANCH => 'main') WHERE product_id = 5;

SELECT '=== ALL MERGE ISOLATION CHECKS COMPLETE ===' as msg;
