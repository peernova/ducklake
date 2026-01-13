-- Worker: Runs compaction with artificial delay DURING the operation
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=compact_delete_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/compact_delete_test');
USE t;

SELECT 'Compact Worker: Starting immediately, count=' || COUNT(*) as msg FROM orders;
SELECT 'Compact Worker: Running compaction...' as msg;

-- Run compaction - no delay, start immediately
CALL ducklake_merge_adjacent_files('t', 'orders');

SELECT 'Compact Worker: Compaction completed, count=' || COUNT(*) as msg FROM orders;
