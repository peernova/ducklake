-- Worker: Runs delete (starts immediately)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=compact_delete_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/compact_delete_test');
USE t;

SELECT 'Delete Worker: Starting, count=' || COUNT(*) as msg FROM orders;

-- Delete some rows immediately
SELECT 'Delete Worker: Deleting rows where id <= 100...' as msg;
DELETE FROM orders WHERE id <= 100;

SELECT 'Delete Worker: Delete completed, count=' || COUNT(*) as msg FROM orders;
