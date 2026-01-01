-- Verify partition changes work correctly across branches
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=change_partition_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/change_partition_test');
USE t;

SELECT '=== CHANGE PARTITION ON BRANCHES VERIFICATION ===' as msg;

-- Test queries with partition filters on each branch
SELECT '--- Main Branch (partition by event_type) ---' as msg;
SELECT event_type, COUNT(*) as count, SUM(amount) as total
FROM events AT (BRANCH => 'main')
GROUP BY event_type ORDER BY event_type;

SELECT '--- partition_by_region branch ---' as msg;
SELECT region, COUNT(*) as count, SUM(amount) as total
FROM events AT (BRANCH => 'partition_by_region')
GROUP BY region ORDER BY region;

SELECT '--- partition_by_date branch ---' as msg;
SELECT event_date, COUNT(*) as count, SUM(amount) as total
FROM events AT (BRANCH => 'partition_by_date')
GROUP BY event_date ORDER BY event_date;

SELECT '=== PARTITION FILTER TESTS ===' as msg;

-- Filter by event_type on main
SELECT 'Main filter by sale:' as branch, COUNT(*) as count
FROM events AT (BRANCH => 'main') WHERE event_type = 'sale';

-- Filter by region on partition_by_region
SELECT 'Region filter by US:' as branch, COUNT(*) as count
FROM events AT (BRANCH => 'partition_by_region') WHERE region = 'US';

-- Filter by date on partition_by_date
SELECT 'Date filter by 2024-01-15:' as branch, COUNT(*) as count
FROM events AT (BRANCH => 'partition_by_date') WHERE event_date = '2024-01-15';

SELECT '=== SUMMARY ===' as msg;

-- Verify all branches have same row count
SELECT CASE WHEN COUNT(*) = 6 THEN 'PASS' ELSE 'FAIL' END || ': main has 6 events = ' || COUNT(*)::VARCHAR
FROM events AT (BRANCH => 'main');

SELECT CASE WHEN COUNT(*) = 6 THEN 'PASS' ELSE 'FAIL' END || ': partition_by_region has 6 events = ' || COUNT(*)::VARCHAR
FROM events AT (BRANCH => 'partition_by_region');

SELECT CASE WHEN COUNT(*) = 6 THEN 'PASS' ELSE 'FAIL' END || ': partition_by_date has 6 events = ' || COUNT(*)::VARCHAR
FROM events AT (BRANCH => 'partition_by_date');

-- Verify partition filters return correct counts
SELECT CASE WHEN COUNT(*) = 4 THEN 'PASS' ELSE 'FAIL' END || ': main event_type=sale filter = ' || COUNT(*)::VARCHAR
FROM events AT (BRANCH => 'main') WHERE event_type = 'sale';

SELECT CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END || ': partition_by_region region=US filter = ' || COUNT(*)::VARCHAR
FROM events AT (BRANCH => 'partition_by_region') WHERE region = 'US';

SELECT CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END || ': partition_by_date date=2024-01-15 filter = ' || COUNT(*)::VARCHAR
FROM events AT (BRANCH => 'partition_by_date') WHERE event_date = '2024-01-15';

-- Verify data consistency across branches (same totals)
SELECT CASE WHEN m.total = r.total AND r.total = d.total THEN 'PASS' ELSE 'FAIL' END
    || ': All branches have same total amount = ' || m.total::VARCHAR
FROM (SELECT SUM(amount) as total FROM events AT (BRANCH => 'main')) m,
     (SELECT SUM(amount) as total FROM events AT (BRANCH => 'partition_by_region')) r,
     (SELECT SUM(amount) as total FROM events AT (BRANCH => 'partition_by_date')) d;

SELECT '=== ALL TESTS COMPLETE ===' as msg;
