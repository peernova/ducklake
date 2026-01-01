-- Verify partitioning works correctly across branches
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=partition_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/partition_test');
USE t;

SELECT '=== PARTITIONING WITH BRANCHES VERIFICATION ===' as msg;

-- Check counts per region on main
SELECT '--- Main Branch ---' as msg;
SELECT region, COUNT(*) as count, ROUND(SUM(amount)::NUMERIC, 2) as total
FROM sales AT (BRANCH => 'main')
GROUP BY region ORDER BY region;

-- Check counts per region on us_branch
SELECT '--- US Branch ---' as msg;
SELECT region, COUNT(*) as count, ROUND(SUM(amount)::NUMERIC, 2) as total
FROM sales AT (BRANCH => 'us_branch')
GROUP BY region ORDER BY region;

-- Check counts per region on eu_branch
SELECT '--- EU Branch ---' as msg;
SELECT region, COUNT(*) as count, ROUND(SUM(amount)::NUMERIC, 2) as total
FROM sales AT (BRANCH => 'eu_branch')
GROUP BY region ORDER BY region;

-- Check counts per region on asia_branch
SELECT '--- ASIA Branch ---' as msg;
SELECT region, COUNT(*) as count, ROUND(SUM(amount)::NUMERIC, 2) as total
FROM sales AT (BRANCH => 'asia_branch')
GROUP BY region ORDER BY region;

SELECT '=== PARTITION FILTER TESTS ===' as msg;

-- Test partition pruning with AT BRANCH - should only scan US partition
SELECT '--- Filter by region=US on each branch ---' as msg;
SELECT 'main' as branch, COUNT(*) as us_count FROM sales AT (BRANCH => 'main') WHERE region = 'US';
SELECT 'us_branch' as branch, COUNT(*) as us_count FROM sales AT (BRANCH => 'us_branch') WHERE region = 'US';
SELECT 'eu_branch' as branch, COUNT(*) as us_count FROM sales AT (BRANCH => 'eu_branch') WHERE region = 'US';
SELECT 'asia_branch' as branch, COUNT(*) as us_count FROM sales AT (BRANCH => 'asia_branch') WHERE region = 'US';

SELECT '=== SUMMARY ===' as msg;

-- Verify main has 9 total sales
SELECT CASE WHEN COUNT(*) = 9 THEN 'PASS' ELSE 'FAIL' END || ': main has 9 sales = ' || COUNT(*)::VARCHAR
FROM sales AT (BRANCH => 'main');

-- Verify us_branch has 11 total sales (9 + 2)
SELECT CASE WHEN COUNT(*) = 11 THEN 'PASS' ELSE 'FAIL' END || ': us_branch has 11 sales = ' || COUNT(*)::VARCHAR
FROM sales AT (BRANCH => 'us_branch');

-- Verify eu_branch has 12 total sales (9 + 3)
SELECT CASE WHEN COUNT(*) = 12 THEN 'PASS' ELSE 'FAIL' END || ': eu_branch has 12 sales = ' || COUNT(*)::VARCHAR
FROM sales AT (BRANCH => 'eu_branch');

-- Verify asia_branch has 9 sales (same count, different amounts)
SELECT CASE WHEN COUNT(*) = 9 THEN 'PASS' ELSE 'FAIL' END || ': asia_branch has 9 sales = ' || COUNT(*)::VARCHAR
FROM sales AT (BRANCH => 'asia_branch');

-- Verify US count on us_branch is 5
SELECT CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END || ': us_branch has 5 US sales = ' || COUNT(*)::VARCHAR
FROM sales AT (BRANCH => 'us_branch') WHERE region = 'US';

-- Verify EU count on eu_branch is 6 (3 initial + 3 new)
SELECT CASE WHEN COUNT(*) = 6 THEN 'PASS' ELSE 'FAIL' END || ': eu_branch has 6 EU sales = ' || COUNT(*)::VARCHAR
FROM sales AT (BRANCH => 'eu_branch') WHERE region = 'EU';

-- Verify ASIA prices increased by 10% on asia_branch
SELECT CASE WHEN ABS(a.total - m.total * 1.1) < 0.02 THEN 'PASS' ELSE 'FAIL' END
    || ': asia_branch ASIA prices 10% higher (main=' || ROUND(m.total::NUMERIC, 2)::VARCHAR
    || ', asia=' || ROUND(a.total::NUMERIC, 2)::VARCHAR || ')'
FROM (SELECT SUM(amount) as total FROM sales AT (BRANCH => 'main') WHERE region = 'ASIA') m,
     (SELECT SUM(amount) as total FROM sales AT (BRANCH => 'asia_branch') WHERE region = 'ASIA') a;

-- Verify non-ASIA regions unchanged on asia_branch
SELECT CASE WHEN a.total = m.total THEN 'PASS' ELSE 'FAIL' END
    || ': asia_branch US prices unchanged'
FROM (SELECT SUM(amount) as total FROM sales AT (BRANCH => 'main') WHERE region = 'US') m,
     (SELECT SUM(amount) as total FROM sales AT (BRANCH => 'asia_branch') WHERE region = 'US') a;

SELECT '=== ALL TESTS COMPLETE ===' as msg;
