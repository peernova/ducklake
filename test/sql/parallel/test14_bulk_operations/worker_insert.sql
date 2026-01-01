-- Bulk Insert: INSERT SELECT to double data
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_bulk host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_bulk');
USE t;

CALL ducklake_use_branch('t', 'bulk_insert');
SELECT 'BULK_INSERT: Doubling data with INSERT SELECT...';

-- Double the data
INSERT INTO data SELECT id + 1000, category, value * 2, 'cloned' FROM data;

SELECT 'BULK_INSERT: Final count = ' || COUNT(*)::VARCHAR || ', sum=' || SUM(value)::VARCHAR FROM data;
