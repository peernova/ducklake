-- Verify: Table should have all 4 new columns (email, phone, age, created_at)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=schema_evolution_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/schema_evolution_test');
USE t;

SELECT '=== PARALLEL SCHEMA EVOLUTION VERIFICATION ===' as msg;

-- Show current schema
SELECT '--- Current Schema ---' as msg;
DESCRIBE users;

-- Show data
SELECT '--- Current Data ---' as msg;
SELECT * FROM users ORDER BY id;

-- Count columns
SELECT '--- Column Count ---' as msg;
SELECT COUNT(*) as column_count FROM (DESCRIBE users);

SELECT '=== SUMMARY ===' as msg;

-- Verify we have 6 columns total (id, name, email, phone, age, created_at)
SELECT CASE WHEN COUNT(*) = 6 THEN 'PASS' ELSE 'FAIL' END || ': Table has 6 columns = ' || COUNT(*)::VARCHAR
FROM (DESCRIBE users);

-- Verify original data is preserved
SELECT CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END || ': Original 3 rows preserved = ' || COUNT(*)::VARCHAR
FROM users;

-- Check each column exists
SELECT CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END || ': email column exists'
FROM (DESCRIBE users) WHERE column_name = 'email';

SELECT CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END || ': phone column exists'
FROM (DESCRIBE users) WHERE column_name = 'phone';

SELECT CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END || ': age column exists'
FROM (DESCRIBE users) WHERE column_name = 'age';

SELECT CASE WHEN COUNT(*) = 1 THEN 'PASS' ELSE 'FAIL' END || ': created_at column exists'
FROM (DESCRIBE users) WHERE column_name = 'created_at';
