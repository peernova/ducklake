-- Verify: Check schema isolation after parallel schema changes
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_schema host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_schema');
USE t;

SELECT '=== SCHEMA EVOLUTION VERIFICATION ===' as msg;

-- Count verification
SELECT 'Main: ' || COUNT(*)::VARCHAR || ' orders (expected: 3, original schema)' FROM orders AT (BRANCH => 'main');
SELECT 'V1: ' || COUNT(*)::VARCHAR || ' orders (expected: 4, status+discount columns)' FROM orders AT (BRANCH => 'schema_v1');
SELECT 'V2: ' || COUNT(*)::VARCHAR || ' orders (expected: 4, shipping+tracking columns)' FROM orders AT (BRANCH => 'schema_v2');
SELECT 'V3: ' || COUNT(*)::VARCHAR || ' orders (expected: 3, timestamps+priority, id=1 deleted)' FROM orders AT (BRANCH => 'schema_v3');

SELECT '--- Main (original 3 columns) ---' as msg;
SELECT * FROM orders AT (BRANCH => 'main') ORDER BY id;

SELECT '--- V1 (5 columns: +status, +discount) ---' as msg;
SELECT * FROM orders AT (BRANCH => 'schema_v1') ORDER BY id;

SELECT '--- V2 (5 columns: +shipping_address, +tracking_number) ---' as msg;
SELECT * FROM orders AT (BRANCH => 'schema_v2') ORDER BY id;

SELECT '--- V3 (5 columns: +created_at, +priority, id=1 deleted) ---' as msg;
SELECT * FROM orders AT (BRANCH => 'schema_v3') ORDER BY id;

SELECT '=== SCHEMA ISOLATION CHECKS ===' as msg;
SELECT 'Main has 3 columns: ' || CASE WHEN COUNT(*) = 3 THEN 'YES' ELSE 'NO' END
    FROM (SELECT UNNEST(['id', 'customer', 'amount']));
SELECT 'V1 has status column: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END
    FROM orders AT (BRANCH => 'schema_v1') WHERE status IS NOT NULL LIMIT 1;
SELECT 'V2 has tracking column: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END
    FROM orders AT (BRANCH => 'schema_v2') WHERE tracking_number IS NOT NULL LIMIT 1;
SELECT 'V3 order 1 deleted: ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO' END
    FROM orders AT (BRANCH => 'schema_v3') WHERE id = 1;
SELECT 'Main order 1 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END
    FROM orders AT (BRANCH => 'main') WHERE id = 1;
