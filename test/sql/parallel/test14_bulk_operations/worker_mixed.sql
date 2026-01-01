-- Bulk Mixed: Complex multi-step bulk ops
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_bulk host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_bulk');
USE t;

CALL ducklake_use_branch('t', 'bulk_mixed');
SELECT 'BULK_MIXED: Complex bulk operations...';

-- Step 1: Delete category A (20 rows)
DELETE FROM data WHERE category = 'A';
SELECT 'BULK_MIXED: Deleted category A';

-- Step 2: Double remaining values
UPDATE data SET value = value * 2;
SELECT 'BULK_MIXED: Doubled values';

-- Step 3: Insert aggregated data
INSERT INTO data SELECT 9001, 'SUMMARY', SUM(value), 'summary' FROM data;
SELECT 'BULK_MIXED: Added summary row';

-- Step 4: Delete low value rows
DELETE FROM data WHERE value < 100 AND category != 'SUMMARY';
SELECT 'BULK_MIXED: Deleted low value rows';

SELECT 'BULK_MIXED: Final count = ' || COUNT(*)::VARCHAR || ', sum=' || SUM(value)::VARCHAR FROM data;
