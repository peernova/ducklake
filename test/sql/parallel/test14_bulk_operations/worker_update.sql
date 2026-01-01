-- Bulk Update: UPDATE all rows
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_bulk host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_bulk');
USE t;

CALL ducklake_use_branch('t', 'bulk_update');
SELECT 'BULK_UPDATE: Updating all 100 rows...';

-- Triple all values
UPDATE data SET value = value * 3;
-- Update status
UPDATE data SET status = 'processed';

SELECT 'BULK_UPDATE: Final count = ' || COUNT(*)::VARCHAR || ', sum=' || SUM(value)::VARCHAR FROM data;
