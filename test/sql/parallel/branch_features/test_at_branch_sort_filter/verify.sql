-- Verification: Check branch data isolation and correctness
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=at_branch_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/at_branch_test');
USE t;

SELECT '=== AT BRANCH SORT/FILTER VERIFICATION ===' as msg;

-- Check row counts per branch
SELECT '--- Row Counts ---' as msg;
SELECT 'main' as branch, COUNT(*) as count FROM products AT (BRANCH => 'main');
SELECT 'branch_discount' as branch, COUNT(*) as count FROM products AT (BRANCH => 'branch_discount');
SELECT 'branch_restock' as branch, COUNT(*) as count FROM products AT (BRANCH => 'branch_restock');
SELECT 'branch_new_items' as branch, COUNT(*) as count FROM products AT (BRANCH => 'branch_new_items');

-- Check price aggregates per branch
SELECT '--- Price Aggregates ---' as msg;
SELECT 'main' as branch, ROUND(SUM(price)::NUMERIC, 2) as total_price, ROUND(AVG(price)::NUMERIC, 2) as avg_price FROM products AT (BRANCH => 'main');
SELECT 'branch_discount' as branch, ROUND(SUM(price)::NUMERIC, 2) as total_price, ROUND(AVG(price)::NUMERIC, 2) as avg_price FROM products AT (BRANCH => 'branch_discount');

-- Check stock aggregates per branch
SELECT '--- Stock Aggregates ---' as msg;
SELECT 'main' as branch, SUM(stock) as total_stock FROM products AT (BRANCH => 'main');
SELECT 'branch_restock' as branch, SUM(stock) as total_stock FROM products AT (BRANCH => 'branch_restock');

SELECT '=== SUMMARY ===' as msg;

-- Verify main has 10 products
SELECT CASE WHEN COUNT(*) = 10 THEN 'PASS' ELSE 'FAIL' END || ': main has 10 products = ' || COUNT(*)::VARCHAR
FROM products AT (BRANCH => 'main');

-- Verify branch_discount has 10 products with reduced prices
SELECT CASE WHEN COUNT(*) = 10 THEN 'PASS' ELSE 'FAIL' END || ': branch_discount has 10 products = ' || COUNT(*)::VARCHAR
FROM products AT (BRANCH => 'branch_discount');

-- Verify discount prices are 90% of main prices (use tolerance for decimal rounding)
SELECT CASE WHEN ABS(d.total - m.total * 0.9) < 0.02 THEN 'PASS' ELSE 'FAIL' END
    || ': discount prices are 90% of main (main=' || ROUND(m.total::NUMERIC, 2)::VARCHAR
    || ', discount=' || ROUND(d.total::NUMERIC, 2)::VARCHAR || ')'
FROM (SELECT SUM(price) as total FROM products AT (BRANCH => 'main')) m,
     (SELECT SUM(price) as total FROM products AT (BRANCH => 'branch_discount')) d;

-- Verify branch_restock has 10 products with doubled stock
SELECT CASE WHEN COUNT(*) = 10 THEN 'PASS' ELSE 'FAIL' END || ': branch_restock has 10 products = ' || COUNT(*)::VARCHAR
FROM products AT (BRANCH => 'branch_restock');

-- Verify restock has doubled stock
SELECT CASE WHEN r.total = m.total * 2 THEN 'PASS' ELSE 'FAIL' END
    || ': restock stock is 2x main (main=' || m.total::VARCHAR || ', restock=' || r.total::VARCHAR || ')'
FROM (SELECT SUM(stock) as total FROM products AT (BRANCH => 'main')) m,
     (SELECT SUM(stock) as total FROM products AT (BRANCH => 'branch_restock')) r;

-- Verify branch_new_items has 13 products
SELECT CASE WHEN COUNT(*) = 13 THEN 'PASS' ELSE 'FAIL' END || ': branch_new_items has 13 products = ' || COUNT(*)::VARCHAR
FROM products AT (BRANCH => 'branch_new_items');

-- Verify new items exist only on branch_new_items
SELECT CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END || ': branch_new_items has 3 new items (id > 10) = ' || COUNT(*)::VARCHAR
FROM products AT (BRANCH => 'branch_new_items') WHERE id > 10;

SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END || ': main has no items with id > 10 = ' || COUNT(*)::VARCHAR
FROM products AT (BRANCH => 'main') WHERE id > 10;

-- Verify sort correctness: first product by price on each branch
SELECT '--- Sort Verification ---' as msg;
SELECT CASE WHEN (SELECT id FROM products AT (BRANCH => 'main') ORDER BY price ASC LIMIT 1) = 8 THEN 'PASS' ELSE 'FAIL' END
    || ': main cheapest product is id=8 (Notebook $4.99)';

SELECT CASE WHEN (SELECT id FROM products AT (BRANCH => 'branch_discount') ORDER BY price ASC LIMIT 1) = 8 THEN 'PASS' ELSE 'FAIL' END
    || ': discount cheapest product is id=8 (Notebook $4.49)';

-- Verify filter correctness: count Electronics on each branch
SELECT '--- Filter Verification ---' as msg;
SELECT CASE WHEN COUNT(*) = 4 THEN 'PASS' ELSE 'FAIL' END
    || ': main has 4 Electronics = ' || COUNT(*)::VARCHAR
FROM products AT (BRANCH => 'main') WHERE category = 'Electronics';

SELECT CASE WHEN COUNT(*) = 6 THEN 'PASS' ELSE 'FAIL' END
    || ': branch_new_items has 6 Electronics = ' || COUNT(*)::VARCHAR
FROM products AT (BRANCH => 'branch_new_items') WHERE category = 'Electronics';

SELECT '=== ALL TESTS COMPLETE ===' as msg;
