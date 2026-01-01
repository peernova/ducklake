-- Bulk Delete: DELETE 50% of rows
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_bulk host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_bulk');
USE t;

CALL ducklake_use_branch('t', 'bulk_delete');
SELECT 'BULK_DELETE: Removing 50% of data...';

-- Delete odd IDs (50 rows)
DELETE FROM data WHERE id % 2 = 1;

SELECT 'BULK_DELETE: Final count = ' || COUNT(*)::VARCHAR || ', sum=' || SUM(value)::VARCHAR FROM data;
