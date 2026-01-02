-- Verify isolation: Each branch should have its own state, main unchanged
ATTACH 'ducklake:metadata=postgres:host=localhost port=5433 dbname=parallel_drop' AS t (DATA_PATH '/tmp/parallel_drop');
USE t;

SELECT '=== VERIFICATION: DROP OPERATIONS ISOLATION ===' as msg;

-- ============================================
-- MAIN BRANCH: Should have ALL original objects
-- ============================================
CALL ducklake_use_branch('t', 'main');
SELECT '--- MAIN BRANCH (should be unchanged) ---' as msg;

SELECT 'Main schemas:' as msg;
SELECT schema_name FROM information_schema.schemata
WHERE catalog_name = 't' AND schema_name NOT IN ('information_schema', 'pg_catalog')
ORDER BY schema_name;

SELECT 'Main: sales.products exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END as check
FROM information_schema.tables WHERE table_schema = 'sales' AND table_name = 'products' AND table_catalog = 't';

SELECT 'Main: analytics.order_summary exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END as check
FROM information_schema.tables WHERE table_schema = 'analytics' AND table_name = 'order_summary' AND table_catalog = 't';

SELECT 'Main: reporting schema exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END as check
FROM information_schema.schemata WHERE schema_name = 'reporting' AND catalog_name = 't';

SELECT 'Main: inventory schema exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END as check
FROM information_schema.schemata WHERE schema_name = 'inventory' AND catalog_name = 't';

SELECT 'Main: sales.customers exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END as check
FROM information_schema.tables WHERE table_schema = 'sales' AND table_name = 'customers' AND table_catalog = 't';

-- Verify main data unchanged
SELECT 'Main: orders count = ' || COUNT(*) || ' (expected 3)' as check FROM sales.orders;
SELECT 'Main: products count = ' || COUNT(*) || ' (expected 2)' as check FROM sales.products;

-- ============================================
-- BRANCH_A: sales.products dropped
-- ============================================
CALL ducklake_use_branch('t', 'branch_a');
SELECT '--- BRANCH_A (sales.products dropped) ---' as msg;

SELECT 'Branch_A: sales.products exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - WRONG!' ELSE 'NO - correct' END as check
FROM information_schema.tables WHERE table_schema = 'sales' AND table_name = 'products' AND table_catalog = 't';

SELECT 'Branch_A: orders count = ' || COUNT(*) || ' (expected 4 with new order)' as check FROM sales.orders;

-- All other objects should exist
SELECT 'Branch_A: analytics.order_summary exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END as check
FROM information_schema.tables WHERE table_schema = 'analytics' AND table_name = 'order_summary' AND table_catalog = 't';

-- ============================================
-- BRANCH_B: analytics.order_summary dropped
-- ============================================
CALL ducklake_use_branch('t', 'branch_b');
SELECT '--- BRANCH_B (analytics.order_summary dropped) ---' as msg;

SELECT 'Branch_B: analytics.order_summary exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - WRONG!' ELSE 'NO - correct' END as check
FROM information_schema.tables WHERE table_schema = 'analytics' AND table_name = 'order_summary' AND table_catalog = 't';

SELECT 'Branch_B: analytics.order_summary_v2 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - correct' ELSE 'NO - WRONG!' END as check
FROM information_schema.tables WHERE table_schema = 'analytics' AND table_name = 'order_summary_v2' AND table_catalog = 't';

-- sales.products should still exist
SELECT 'Branch_B: sales.products exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END as check
FROM information_schema.tables WHERE table_schema = 'sales' AND table_name = 'products' AND table_catalog = 't';

-- ============================================
-- BRANCH_C: reporting schema dropped
-- ============================================
CALL ducklake_use_branch('t', 'branch_c');
SELECT '--- BRANCH_C (reporting schema dropped) ---' as msg;

SELECT 'Branch_C: reporting schema exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - WRONG!' ELSE 'NO - correct' END as check
FROM information_schema.schemata WHERE schema_name = 'reporting' AND catalog_name = 't';

SELECT 'Branch_C: branch_c_reports schema exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - correct' ELSE 'NO - WRONG!' END as check
FROM information_schema.schemata WHERE schema_name = 'branch_c_reports' AND catalog_name = 't';

-- Other schemas should exist
SELECT 'Branch_C: sales schema exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END as check
FROM information_schema.schemata WHERE schema_name = 'sales' AND catalog_name = 't';

SELECT 'Branch_C: inventory schema exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END as check
FROM information_schema.schemata WHERE schema_name = 'inventory' AND catalog_name = 't';

-- ============================================
-- BRANCH_D: multiple drops (customers, top_customers, inventory)
-- ============================================
CALL ducklake_use_branch('t', 'branch_d');
SELECT '--- BRANCH_D (multiple drops) ---' as msg;

SELECT 'Branch_D: sales.customers exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - WRONG!' ELSE 'NO - correct' END as check
FROM information_schema.tables WHERE table_schema = 'sales' AND table_name = 'customers' AND table_catalog = 't';

SELECT 'Branch_D: analytics.top_customers exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - WRONG!' ELSE 'NO - correct' END as check
FROM information_schema.tables WHERE table_schema = 'analytics' AND table_name = 'top_customers' AND table_catalog = 't';

SELECT 'Branch_D: inventory schema exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - WRONG!' ELSE 'NO - correct' END as check
FROM information_schema.schemata WHERE schema_name = 'inventory' AND catalog_name = 't';

-- reporting should still exist on branch_d
SELECT 'Branch_D: reporting schema exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - correct' ELSE 'NO - WRONG!' END as check
FROM information_schema.schemata WHERE schema_name = 'reporting' AND catalog_name = 't';

SELECT 'Branch_D: orders count = ' || COUNT(*) || ' (expected 4 with new order)' as check FROM sales.orders;

-- ============================================
-- CROSS-BRANCH TIME TRAVEL
-- ============================================
SELECT '--- TIME TRAVEL ACROSS BRANCHES ---' as msg;

-- From branch_d, access main's products (which branch_a dropped but main still has)
SELECT 'From branch_d, access main products via time travel:' as msg;
SELECT * FROM sales.products AT (BRANCH => 'main') ORDER BY id;

-- From main, access branch_c's new schema
CALL ducklake_use_branch('t', 'main');
SELECT 'From main, access branch_c new schema via time travel:' as msg;
SELECT * FROM branch_c_reports.custom_stats AT (BRANCH => 'branch_c') ORDER BY id;

SELECT '=== ALL ISOLATION CHECKS COMPLETE ===' as msg;
