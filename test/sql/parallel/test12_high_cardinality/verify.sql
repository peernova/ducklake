-- Verify: High-cardinality partition isolation
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_high_card host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_high_card');
USE t;

SELECT '=== HIGH-CARDINALITY PARTITION VERIFICATION ===' as msg;

SELECT 'Main: ' || COUNT(*)::VARCHAR || ' rows, ' || COUNT(DISTINCT country)::VARCHAR || ' countries (expected: 30, 15)' FROM events AT (BRANCH => 'main');
SELECT 'Americas: ' || COUNT(*)::VARCHAR || ' rows' FROM events AT (BRANCH => 'americas');
SELECT 'Europe: ' || COUNT(*)::VARCHAR || ' rows' FROM events AT (BRANCH => 'europe');
SELECT 'AsiaPac: ' || COUNT(*)::VARCHAR || ' rows' FROM events AT (BRANCH => 'asiapac');
SELECT 'Global: ' || COUNT(*)::VARCHAR || ' rows' FROM events AT (BRANCH => 'global_ops');
SELECT 'New_regions: ' || COUNT(*)::VARCHAR || ' rows, ' || COUNT(DISTINCT country)::VARCHAR || ' countries (expected: 40, 20)' FROM events AT (BRANCH => 'new_regions');

SELECT '=== ISOLATION CHECKS ===' as msg;
-- Main unchanged
SELECT 'Main US value unchanged (150): ' || CASE WHEN SUM(value) = 150 THEN 'YES' ELSE 'NO - ' || SUM(value)::VARCHAR END FROM events AT (BRANCH => 'main') WHERE country = 'US';
-- Americas doubled US
SELECT 'Americas US value doubled+500: ' || CASE WHEN SUM(value) > 500 THEN 'YES' ELSE 'NO' END FROM events AT (BRANCH => 'americas') WHERE country = 'US';
-- Europe deleted ES
SELECT 'Europe deleted ES: ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO' END FROM events AT (BRANCH => 'europe') WHERE country = 'ES';
-- Main still has ES
SELECT 'Main still has ES: ' || CASE WHEN COUNT(*) = 2 THEN 'YES' ELSE 'NO' END FROM events AT (BRANCH => 'main') WHERE country = 'ES';
-- New_regions has 20 countries
SELECT 'New_regions has 20 countries: ' || CASE WHEN COUNT(DISTINCT country) = 20 THEN 'YES' ELSE 'NO - ' || COUNT(DISTINCT country)::VARCHAR END FROM events AT (BRANCH => 'new_regions');
-- Main has only 15 countries
SELECT 'Main has only 15 countries: ' || CASE WHEN COUNT(DISTINCT country) = 15 THEN 'YES' ELSE 'NO' END FROM events AT (BRANCH => 'main');
-- Global renamed view to impression
SELECT 'Global has impressions: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END FROM events AT (BRANCH => 'global_ops') WHERE event_type = 'impression';
-- Main still has views
SELECT 'Main still has views: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END FROM events AT (BRANCH => 'main') WHERE event_type = 'view';
