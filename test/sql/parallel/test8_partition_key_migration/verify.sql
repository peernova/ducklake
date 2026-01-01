-- Verify: Each branch has its own partition distribution after migrations
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_partition_migration host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_partition_migration');
USE t;

SELECT '=== PARTITION KEY MIGRATION VERIFICATION ===' as msg;

SELECT '--- Main Branch (UNCHANGED) ---' as msg;
SELECT status, COUNT(*) as cnt FROM orders AT (BRANCH => 'main') GROUP BY status ORDER BY status;

SELECT '--- Migrate_A (pending -> processing) ---' as msg;
SELECT status, COUNT(*) as cnt FROM orders AT (BRANCH => 'migrate_a') GROUP BY status ORDER BY status;

SELECT '--- Migrate_B (processing -> shipped) ---' as msg;
SELECT status, COUNT(*) as cnt FROM orders AT (BRANCH => 'migrate_b') GROUP BY status ORDER BY status;

SELECT '--- Migrate_C (shipped -> delivered) ---' as msg;
SELECT status, COUNT(*) as cnt FROM orders AT (BRANCH => 'migrate_c') GROUP BY status ORDER BY status;

SELECT '--- Migrate_D (ALL -> archived) ---' as msg;
SELECT status, COUNT(*) as cnt FROM orders AT (BRANCH => 'migrate_d') GROUP BY status ORDER BY status;

SELECT '=== EXPECTED DISTRIBUTIONS ===' as msg;
-- Main: pending=3, processing=2, shipped=2, delivered=1
SELECT 'Main expected: pending=3, processing=2, shipped=2, delivered=1' as expected;
-- Migrate_A: pending=0, processing=5, shipped=2, delivered=1
SELECT 'Migrate_A expected: pending=0, processing=5, shipped=2, delivered=1' as expected;
-- Migrate_B: pending=3, processing=0, shipped=4, delivered=1
SELECT 'Migrate_B expected: pending=3, processing=0, shipped=4, delivered=1' as expected;
-- Migrate_C: pending=3, processing=2, shipped=0, delivered=3
SELECT 'Migrate_C expected: pending=3, processing=2, shipped=0, delivered=3' as expected;
-- Migrate_D: archived=8
SELECT 'Migrate_D expected: archived=8' as expected;

SELECT '=== ISOLATION CHECKS ===' as msg;
-- Main has pending
SELECT 'Main has pending orders: ' || CASE WHEN COUNT(*) = 3 THEN 'YES (3)' ELSE 'NO - ERROR!' END FROM orders AT (BRANCH => 'main') WHERE status = 'pending';
-- Migrate_A has no pending
SELECT 'Migrate_A has NO pending: ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO - ERROR!' END FROM orders AT (BRANCH => 'migrate_a') WHERE status = 'pending';
-- Migrate_B has no processing
SELECT 'Migrate_B has NO processing: ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO - ERROR!' END FROM orders AT (BRANCH => 'migrate_b') WHERE status = 'processing';
-- Migrate_C has no shipped
SELECT 'Migrate_C has NO shipped: ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO - ERROR!' END FROM orders AT (BRANCH => 'migrate_c') WHERE status = 'shipped';
-- Migrate_D has ONLY archived
SELECT 'Migrate_D has ONLY archived: ' || CASE WHEN COUNT(*) = 8 THEN 'YES (8)' ELSE 'NO - ERROR!' END FROM orders AT (BRANCH => 'migrate_d') WHERE status = 'archived';
-- Main has NO archived
SELECT 'Main has NO archived: ' || CASE WHEN COUNT(*) = 0 THEN 'YES - isolated' ELSE 'NO - LEAK!' END FROM orders AT (BRANCH => 'main') WHERE status = 'archived';
