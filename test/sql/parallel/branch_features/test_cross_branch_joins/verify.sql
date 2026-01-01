-- Cross-Branch Joins and Multi-Catalog Verification
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=catalog_sales host=localhost port=5433 user=postgres password=postgres' AS sales (DATA_PATH '/tmp/catalog_sales');
ATTACH 'ducklake:postgres:dbname=catalog_inventory host=localhost port=5433 user=postgres password=postgres' AS inventory (DATA_PATH '/tmp/catalog_inventory');

SELECT '=== CROSS-BRANCH JOINS TEST ===' as msg;

-- ========== TEST 1: Join within same catalog, same branch ==========
SELECT '--- Test 1: Same catalog, same branch join ---' as msg;
SELECT c.name, COUNT(o.id) as order_count, SUM(o.total) as total_spent
FROM sales.main.customers AS c AT (BRANCH => 'main')
JOIN sales.main.orders AS o AT (BRANCH => 'main') ON c.id = o.customer_id
GROUP BY c.name
ORDER BY total_spent DESC;

-- ========== TEST 2: Join within same catalog, DIFFERENT branches ==========
SELECT '--- Test 2: Same catalog, cross-branch join (promo customers + main orders) ---' as msg;
-- Join promo_branch customers (upgraded tiers) with main branch orders
SELECT c.name, c.tier as promo_tier, COUNT(o.id) as order_count
FROM sales.main.customers AS c AT (BRANCH => 'promo_branch')
JOIN sales.main.orders AS o AT (BRANCH => 'main') ON c.id = o.customer_id
WHERE c.tier = 'platinum'
GROUP BY c.name, c.tier
ORDER BY c.name;

-- ========== TEST 3: Join across DIFFERENT catalogs, same branch ==========
SELECT '--- Test 3: Cross-catalog join (sales + inventory on main) ---' as msg;
SELECT c.name as customer, p.name as product, o.quantity, o.total,
       (SELECT SUM(s.quantity) FROM inventory.main.stock AS s AT (BRANCH => 'main') WHERE s.product_id = p.id) as total_stock
FROM sales.main.orders AS o AT (BRANCH => 'main')
JOIN sales.main.customers AS c AT (BRANCH => 'main') ON o.customer_id = c.id
JOIN inventory.main.products AS p AT (BRANCH => 'main') ON o.product_id = p.id
WHERE o.status = 'completed'
ORDER BY o.total DESC
LIMIT 5;

-- ========== TEST 4: Join across catalogs with DIFFERENT branches each ==========
SELECT '--- Test 4: Cross-catalog, cross-branch join ---' as msg;
-- Sales promo_branch (platinum customers) + Inventory sale_branch (discounted prices)
SELECT c.name, c.tier, p.name as product, p.price as sale_price, o.quantity
FROM sales.main.customers AS c AT (BRANCH => 'promo_branch')
JOIN sales.main.orders AS o AT (BRANCH => 'promo_branch') ON c.id = o.customer_id
JOIN inventory.main.products AS p AT (BRANCH => 'sale_branch') ON o.product_id = p.id
WHERE c.tier = 'platinum'
ORDER BY c.name, p.name;

-- ========== TEST 5: Aggregate across branches ==========
SELECT '--- Test 5: Compare totals across branches ---' as msg;
SELECT 'main' as branch, SUM(total) as order_total FROM sales.main.orders AT (BRANCH => 'main')
UNION ALL
SELECT 'promo_branch', SUM(total) FROM sales.main.orders AT (BRANCH => 'promo_branch')
UNION ALL
SELECT 'cleanup_branch', SUM(total) FROM sales.main.orders AT (BRANCH => 'cleanup_branch')
ORDER BY 1;

-- ========== TEST 6: Subquery with different branch ==========
SELECT '--- Test 6: Subquery with different branch ---' as msg;
-- Find products from new_products_branch that don't exist in main
SELECT p.id, p.name, p.price
FROM inventory.main.products AS p AT (BRANCH => 'new_products_branch')
WHERE p.id NOT IN (
    SELECT id FROM inventory.main.products AT (BRANCH => 'main')
)
ORDER BY p.id;

-- ========== TEST 7: Three-way cross-catalog cross-branch join ==========
SELECT '--- Test 7: Complex 3-way join across catalogs and branches ---' as msg;
SELECT
    c.name as customer,
    c.tier,
    p.name as product,
    p.price as current_price,
    o.quantity,
    s.warehouse,
    s.quantity as stock_available
FROM sales.main.customers AS c AT (BRANCH => 'promo_branch')
JOIN sales.main.orders AS o AT (BRANCH => 'main') ON c.id = o.customer_id
JOIN inventory.main.products AS p AT (BRANCH => 'sale_branch') ON o.product_id = p.id
JOIN inventory.main.stock AS s AT (BRANCH => 'main') ON p.id = s.product_id
WHERE o.status = 'completed' AND s.warehouse = 'NYC'
ORDER BY o.total DESC;

SELECT '=== VERIFICATION SUMMARY ===' as msg;

-- Verify cross-branch join returns expected results
-- Originally: 1 platinum (MegaCorp) + 2 gold upgraded to platinum (Acme, GlobalTrade) = 3
SELECT CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END
    || ': Cross-branch platinum customers = ' || COUNT(*)::VARCHAR
FROM sales.main.customers AT (BRANCH => 'promo_branch')
WHERE tier = 'platinum';

-- Verify cross-catalog join works
SELECT CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END
    || ': Cross-catalog join returns results = ' || COUNT(*)::VARCHAR
FROM sales.main.orders AS o AT (BRANCH => 'main')
JOIN inventory.main.products AS p AT (BRANCH => 'main') ON o.product_id = p.id;

-- Verify new products only in new_products_branch
SELECT CASE WHEN COUNT(*) = 2 THEN 'PASS' ELSE 'FAIL' END
    || ': New products only in new_products_branch = ' || COUNT(*)::VARCHAR
FROM inventory.main.products AS p AT (BRANCH => 'new_products_branch')
WHERE p.id NOT IN (SELECT id FROM inventory.main.products AT (BRANCH => 'main'));

-- Verify sale prices are 20% off
SELECT CASE WHEN ABS(sale.price - main.price * 0.8) < 0.01 THEN 'PASS' ELSE 'FAIL' END
    || ': Sale price is 20% off (main=' || main.price::VARCHAR || ', sale=' || sale.price::VARCHAR || ')'
FROM inventory.main.products AS main AT (BRANCH => 'main')
JOIN inventory.main.products AS sale AT (BRANCH => 'sale_branch') ON main.id = sale.id
WHERE main.id = 101;

-- Verify cleanup_branch has no cancelled orders
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
    || ': Cleanup branch has 0 cancelled orders = ' || COUNT(*)::VARCHAR
FROM sales.main.orders AT (BRANCH => 'cleanup_branch')
WHERE status = 'cancelled';

-- Verify promo_branch has extra order
SELECT CASE WHEN promo.cnt > main.cnt THEN 'PASS' ELSE 'FAIL' END
    || ': Promo branch has more orders (main=' || main.cnt::VARCHAR || ', promo=' || promo.cnt::VARCHAR || ')'
FROM (SELECT COUNT(*) as cnt FROM sales.main.orders AT (BRANCH => 'main')) main,
     (SELECT COUNT(*) as cnt FROM sales.main.orders AT (BRANCH => 'promo_branch')) promo;

-- Verify catalog isolation
SELECT CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END
    || ': Sales catalog has 5 customers = ' || COUNT(*)::VARCHAR
FROM sales.main.customers AT (BRANCH => 'main');

SELECT CASE WHEN COUNT(*) = 4 THEN 'PASS' ELSE 'FAIL' END
    || ': Inventory catalog has 4 products on main = ' || COUNT(*)::VARCHAR
FROM inventory.main.products AT (BRANCH => 'main');

SELECT '=== ALL CROSS-BRANCH JOIN TESTS COMPLETE ===' as msg;
