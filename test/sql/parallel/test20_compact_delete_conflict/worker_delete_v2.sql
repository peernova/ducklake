-- Worker: Runs delete with small delay to ensure overlap
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=compact_delete_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/compact_delete_test');
USE t;

SELECT 'Delete Worker: Starting, count=' || COUNT(*) as msg FROM orders;

-- Small delay to let compact get its snapshot
SELECT pg_sleep(0.1);

-- Delete rows - this should conflict with compaction
SELECT 'Delete Worker: Deleting rows where id <= 500...' as msg;
DELETE FROM orders WHERE id <= 500;

SELECT 'Delete Worker: Delete completed, count=' || COUNT(*) as msg FROM orders;
