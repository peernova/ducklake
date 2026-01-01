-- Test: Filters (WHERE) with AT BRANCH syntax
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=at_branch_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/at_branch_test');
USE t;

SELECT '=== TEST: WHERE FILTERS with AT BRANCH ===' as msg;

-- Test 1: Filter by price range on main branch
SELECT '--- Main branch: Products with price > 100 ---' as msg;
SELECT id, name, price FROM products AT (BRANCH => 'main') WHERE price > 100 ORDER BY price;

-- Test 2: Same filter on discount branch (fewer products should match due to 10% discount)
SELECT '--- branch_discount: Products with price > 100 (after 10% discount) ---' as msg;
SELECT id, name, price FROM products AT (BRANCH => 'branch_discount') WHERE price > 100 ORDER BY price;

-- Test 3: Filter by category on main
SELECT '--- Main branch: Electronics category ---' as msg;
SELECT id, name, price FROM products AT (BRANCH => 'main') WHERE category = 'Electronics' ORDER BY id;

-- Test 4: Same category filter on new_items branch (should have 2 more electronics)
SELECT '--- branch_new_items: Electronics category (includes new items) ---' as msg;
SELECT id, name, price FROM products AT (BRANCH => 'branch_new_items') WHERE category = 'Electronics' ORDER BY id;

-- Test 5: Filter by stock threshold on main
SELECT '--- Main branch: Products with stock >= 100 ---' as msg;
SELECT id, name, stock FROM products AT (BRANCH => 'main') WHERE stock >= 100 ORDER BY stock DESC;

-- Test 6: Same stock filter on restock branch (more products should match due to doubled stock)
SELECT '--- branch_restock: Products with stock >= 100 (after doubling) ---' as msg;
SELECT id, name, stock FROM products AT (BRANCH => 'branch_restock') WHERE stock >= 100 ORDER BY stock DESC;

-- Test 7: Complex filter with AND
SELECT '--- Main branch: Electronics with stock > 50 ---' as msg;
SELECT id, name, category, stock FROM products AT (BRANCH => 'main')
WHERE category = 'Electronics' AND stock > 50 ORDER BY id;

-- Test 8: Complex filter with OR
SELECT '--- Main branch: Price < 20 OR stock > 200 ---' as msg;
SELECT id, name, price, stock FROM products AT (BRANCH => 'main')
WHERE price < 20 OR stock > 200 ORDER BY id;

-- Test 9: Filter using IN
SELECT '--- Main branch: Office and Furniture categories ---' as msg;
SELECT id, name, category, price FROM products AT (BRANCH => 'main')
WHERE category IN ('Office', 'Furniture') ORDER BY category, price;

-- Test 10: Filter with LIKE
SELECT '--- Main branch: Products with 'e' in name ---' as msg;
SELECT id, name FROM products AT (BRANCH => 'main') WHERE name LIKE '%e%' ORDER BY id;

SELECT '=== FILTER TESTS COMPLETE ===' as msg;
