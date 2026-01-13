-- Worker: Runs delete on MANY rows to make it slow enough to overlap
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=compact_delete_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/compact_delete_test');
USE t;

SELECT 'Delete Worker: Starting, count=' || COUNT(*) as msg FROM orders;

-- Delete MANY rows to make operation slower
SELECT 'Delete Worker: Deleting 25000 rows (half the table)...' as msg;
DELETE FROM orders WHERE id <= 25000;

SELECT 'Delete Worker: Delete completed, count=' || COUNT(*) as msg FROM orders;
