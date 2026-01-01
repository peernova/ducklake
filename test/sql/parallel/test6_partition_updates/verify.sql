-- Verify: All branches have isolated partition updates
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_partition_updates host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_partition_updates');
USE t;

SELECT '=== PARTITION UPDATE VERIFICATION ===' as msg;

-- Main should be UNCHANGED (original prices)
SELECT 'Main: ' || COUNT(*)::VARCHAR || ' rows, total = ' || SUM(amount)::VARCHAR || ' (expected: 9 rows, 5410.00)' FROM sales AT (BRANCH => 'main');

-- US Team: +10% on US only, +5 qty, +1 product
-- Original US: 1000+500+300=1800 -> 1800*1.10=1980 + 400 (Monitor) = 2380
SELECT 'US_team: ' || COUNT(*)::VARCHAR || ' rows, US total = ' || SUM(amount)::VARCHAR || ' (expected: 10 rows, US=2380)' FROM sales AT (BRANCH => 'us_team');

-- EU Team: +20% on EU only, -1 product (Tablet removed)
-- Original EU: 1100+550+330=1980, after -Tablet: 1100+550=1650 -> 1650*1.20=1980
SELECT 'EU_team: ' || COUNT(*)::VARCHAR || ' rows, EU total = ' || SUM(amount)::VARCHAR || ' (expected: 8 rows, EU=1980)' FROM sales AT (BRANCH => 'eu_team');

-- ASIA Team: +15% on ASIA, +2 products
-- Original ASIA: 900+450+270=1620 -> 1620*1.15=1863 + 200 + 100 = 2163
SELECT 'ASIA_team: ' || COUNT(*)::VARCHAR || ' rows, ASIA total = ' || SUM(amount)::VARCHAR || ' (expected: 11 rows, ASIA=2163)' FROM sales AT (BRANCH => 'asia_team');

-- Global Team: +5% on all + $10 shipping + LATAM
-- Original: 5410 -> 5410*1.05=5680.50 + 90 (9*$10) + 950 = 6720.50
SELECT 'Global_team: ' || COUNT(*)::VARCHAR || ' rows, total = ' || SUM(amount)::VARCHAR || ' (expected: 10 rows, ~6720.50)' FROM sales AT (BRANCH => 'global_team');

SELECT '--- Main Branch (UNCHANGED) ---' as msg;
SELECT region, product, amount, quantity FROM sales AT (BRANCH => 'main') ORDER BY region, id;

SELECT '--- US Team Branch ---' as msg;
SELECT region, product, amount, quantity FROM sales AT (BRANCH => 'us_team') WHERE region = 'US' ORDER BY id;

SELECT '--- EU Team Branch ---' as msg;
SELECT region, product, amount, quantity FROM sales AT (BRANCH => 'eu_team') WHERE region = 'EU' ORDER BY id;

SELECT '--- ASIA Team Branch ---' as msg;
SELECT region, product, amount, quantity FROM sales AT (BRANCH => 'asia_team') WHERE region = 'ASIA' ORDER BY id;

SELECT '--- Global Team Branch (all regions) ---' as msg;
SELECT region, COUNT(*) as cnt, SUM(amount)::VARCHAR as total FROM sales AT (BRANCH => 'global_team') GROUP BY region ORDER BY region;

SELECT '=== ISOLATION CHECKS ===' as msg;
-- Main unchanged
SELECT 'Main US price unchanged (Laptop=1000): ' || CASE WHEN amount = 1000.00 THEN 'YES' ELSE 'NO - LEAK!' END FROM sales AT (BRANCH => 'main') WHERE id = 1;
-- US team only affected US
SELECT 'US_team EU unchanged (Laptop=1100): ' || CASE WHEN amount = 1100.00 THEN 'YES' ELSE 'NO - LEAK!' END FROM sales AT (BRANCH => 'us_team') WHERE id = 4;
-- EU team only affected EU
SELECT 'EU_team US unchanged (Laptop=1000): ' || CASE WHEN amount = 1000.00 THEN 'YES' ELSE 'NO - LEAK!' END FROM sales AT (BRANCH => 'eu_team') WHERE id = 1;
-- ASIA team only affected ASIA
SELECT 'ASIA_team EU unchanged (Laptop=1100): ' || CASE WHEN amount = 1100.00 THEN 'YES' ELSE 'NO - LEAK!' END FROM sales AT (BRANCH => 'asia_team') WHERE id = 4;
-- Global team has LATAM
SELECT 'Global_team has LATAM: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END FROM sales AT (BRANCH => 'global_team') WHERE region = 'LATAM';
-- Main does NOT have LATAM
SELECT 'Main has NO LATAM: ' || CASE WHEN COUNT(*) = 0 THEN 'YES - isolated' ELSE 'NO - LEAK!' END FROM sales AT (BRANCH => 'main') WHERE region = 'LATAM';
