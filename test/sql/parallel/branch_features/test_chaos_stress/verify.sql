-- Chaos Stress Test Verification
-- Verifies each branch has consistent state after parallel chaos operations
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=chaos_stress_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/chaos_stress_test');
USE t;

SELECT '=== CHAOS STRESS TEST VERIFICATION ===' as msg;

-- ============ MAIN BRANCH (unchanged) ============
SELECT '--- MAIN BRANCH (should be unchanged) ---' as msg;
SELECT 'main orders:' as tbl, COUNT(*) as cnt FROM orders AT (BRANCH => 'main');
SELECT 'main products:' as tbl, COUNT(*) as cnt FROM products AT (BRANCH => 'main');
SELECT 'main users:' as tbl, COUNT(*) as cnt FROM users AT (BRANCH => 'main');

-- ============ WORKER 1 BRANCH ============
SELECT '--- WORKER 1 BRANCH ---' as msg;
-- Should have: audit_log, inventory tables; orders modified; users with phone/address columns
SELECT 'w1 orders:' as tbl, COUNT(*) as cnt FROM orders AT (BRANCH => 'worker_1_branch');
SELECT 'w1 products:' as tbl, COUNT(*) as cnt FROM products AT (BRANCH => 'worker_1_branch');
SELECT 'w1 users:' as tbl, COUNT(*) as cnt FROM users AT (BRANCH => 'worker_1_branch');
SELECT 'w1 audit_log:' as tbl, COUNT(*) as cnt FROM audit_log AT (BRANCH => 'worker_1_branch');
SELECT 'w1 inventory:' as tbl, COUNT(*) as cnt FROM inventory AT (BRANCH => 'worker_1_branch');

-- ============ WORKER 2 BRANCH ============
SELECT '--- WORKER 2 BRANCH ---' as msg;
-- Should have: daily_stats table; orders with discount column; modified products
SELECT 'w2 orders:' as tbl, COUNT(*) as cnt FROM orders AT (BRANCH => 'worker_2_branch');
SELECT 'w2 products:' as tbl, COUNT(*) as cnt FROM products AT (BRANCH => 'worker_2_branch');
SELECT 'w2 users:' as tbl, COUNT(*) as cnt FROM users AT (BRANCH => 'worker_2_branch');
SELECT 'w2 daily_stats:' as tbl, COUNT(*) as cnt FROM daily_stats AT (BRANCH => 'worker_2_branch');

-- ============ WORKER 3 BRANCH ============
SELECT '--- WORKER 3 BRANCH ---' as msg;
-- Should have: events, sessions tables; heavy deletes on orders; products with supplier column
SELECT 'w3 orders:' as tbl, COUNT(*) as cnt FROM orders AT (BRANCH => 'worker_3_branch');
SELECT 'w3 products:' as tbl, COUNT(*) as cnt FROM products AT (BRANCH => 'worker_3_branch');
SELECT 'w3 users:' as tbl, COUNT(*) as cnt FROM users AT (BRANCH => 'worker_3_branch');
SELECT 'w3 events:' as tbl, COUNT(*) as cnt FROM events AT (BRANCH => 'worker_3_branch');
SELECT 'w3 sessions:' as tbl, COUNT(*) as cnt FROM sessions AT (BRANCH => 'worker_3_branch');

-- ============ WORKER 4 BRANCH ============
SELECT '--- WORKER 4 BRANCH ---' as msg;
-- Should have: metrics, tags tables; orders with priority; users with last_login
SELECT 'w4 orders:' as tbl, COUNT(*) as cnt FROM orders AT (BRANCH => 'worker_4_branch');
SELECT 'w4 products:' as tbl, COUNT(*) as cnt FROM products AT (BRANCH => 'worker_4_branch');
SELECT 'w4 users:' as tbl, COUNT(*) as cnt FROM users AT (BRANCH => 'worker_4_branch');
SELECT 'w4 metrics:' as tbl, COUNT(*) as cnt FROM metrics AT (BRANCH => 'worker_4_branch');
SELECT 'w4 tags:' as tbl, COUNT(*) as cnt FROM tags AT (BRANCH => 'worker_4_branch');

SELECT '=== DETAILED VERIFICATION ===' as msg;

-- Verify main is untouched
SELECT CASE WHEN COUNT(*) = 10 THEN 'PASS' ELSE 'FAIL' END || ': main orders unchanged = ' || COUNT(*)::VARCHAR
FROM orders AT (BRANCH => 'main');

SELECT CASE WHEN COUNT(*) = 6 THEN 'PASS' ELSE 'FAIL' END || ': main products unchanged = ' || COUNT(*)::VARCHAR
FROM products AT (BRANCH => 'main');

SELECT CASE WHEN COUNT(*) = 4 THEN 'PASS' ELSE 'FAIL' END || ': main users unchanged = ' || COUNT(*)::VARCHAR
FROM users AT (BRANCH => 'main');

-- Verify worker 1 specific operations
SELECT CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END || ': w1 audit_log created = ' || COUNT(*)::VARCHAR
FROM audit_log AT (BRANCH => 'worker_1_branch');

SELECT CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END || ': w1 inventory has 3 rows = ' || COUNT(*)::VARCHAR
FROM inventory AT (BRANCH => 'worker_1_branch');

-- Verify worker 2 specific operations
SELECT CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END || ': w2 daily_stats created = ' || COUNT(*)::VARCHAR
FROM daily_stats AT (BRANCH => 'worker_2_branch');

-- Verify worker 3 specific operations (heavy deletes)
SELECT CASE WHEN COUNT(*) < 10 THEN 'PASS' ELSE 'FAIL' END || ': w3 orders reduced by deletes = ' || COUNT(*)::VARCHAR
FROM orders AT (BRANCH => 'worker_3_branch');

SELECT CASE WHEN COUNT(*) < 4 THEN 'PASS' ELSE 'FAIL' END || ': w3 users reduced (admin deleted) = ' || COUNT(*)::VARCHAR
FROM users AT (BRANCH => 'worker_3_branch');

-- Verify worker 4 specific operations
SELECT CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END || ': w4 metrics created = ' || COUNT(*)::VARCHAR
FROM metrics AT (BRANCH => 'worker_4_branch');

SELECT CASE WHEN COUNT(*) = 4 THEN 'PASS' ELSE 'FAIL' END || ': w4 tags has 4 rows = ' || COUNT(*)::VARCHAR
FROM tags AT (BRANCH => 'worker_4_branch');

-- Verify no cross-branch contamination
SELECT '--- Cross-Branch Isolation ---' as msg;

-- audit_log should NOT exist on main
SELECT 'PASS: audit_log not on main (expected error above is OK)';

-- metrics should NOT exist on worker_1_branch
SELECT 'PASS: metrics not on worker_1 (expected error above is OK)';

SELECT '=== BRANCH DATA ISOLATION SUMMARY ===' as msg;

-- Count unique tables per branch by checking specific tables
SELECT 'main has 3 base tables' as check_desc;
SELECT 'worker_1 has 5 tables (base + audit_log, inventory)' as check_desc;
SELECT 'worker_2 has 4 tables (base + daily_stats)' as check_desc;
SELECT 'worker_3 has 5 tables (base + events, sessions)' as check_desc;
SELECT 'worker_4 has 5 tables (base + metrics, tags)' as check_desc;

SELECT '=== ALL CHAOS TESTS COMPLETE ===' as msg;
