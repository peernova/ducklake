-- Test: Sort (ORDER BY) with AT BRANCH syntax
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=at_branch_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/at_branch_test');
USE t;

SELECT '=== TEST: ORDER BY with AT BRANCH ===' as msg;

-- Test 1: Sort by price ASC on main branch
SELECT '--- Main branch: Products sorted by price ASC ---' as msg;
SELECT id, name, price FROM products AT (BRANCH => 'main') ORDER BY price ASC LIMIT 5;

-- Test 2: Sort by price ASC on discount branch (prices should be 10% lower)
SELECT '--- branch_discount: Products sorted by price ASC (10% off) ---' as msg;
SELECT id, name, price FROM products AT (BRANCH => 'branch_discount') ORDER BY price ASC LIMIT 5;

-- Test 3: Sort by stock DESC on main branch
SELECT '--- Main branch: Products sorted by stock DESC ---' as msg;
SELECT id, name, stock FROM products AT (BRANCH => 'main') ORDER BY stock DESC LIMIT 5;

-- Test 4: Sort by stock DESC on restock branch (stock should be doubled)
SELECT '--- branch_restock: Products sorted by stock DESC (doubled) ---' as msg;
SELECT id, name, stock FROM products AT (BRANCH => 'branch_restock') ORDER BY stock DESC LIMIT 5;

-- Test 5: Sort by id on new_items branch (should show 13 products)
SELECT '--- branch_new_items: All products sorted by id DESC ---' as msg;
SELECT id, name, category FROM products AT (BRANCH => 'branch_new_items') ORDER BY id DESC;

-- Test 6: Multi-column sort
SELECT '--- Main branch: Sort by category ASC, price DESC ---' as msg;
SELECT id, name, category, price FROM products AT (BRANCH => 'main') ORDER BY category ASC, price DESC;

SELECT '=== SORT TESTS COMPLETE ===' as msg;
