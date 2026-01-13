-- Worker: Runs compaction (will sleep first to let delete start)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=compact_delete_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/compact_delete_test');
USE t;

SELECT 'Compact Worker: Starting, count=' || COUNT(*) as msg FROM orders;

-- Sleep to let delete worker get ahead
SELECT pg_sleep(0.5);

SELECT 'Compact Worker: Running compaction...' as msg;
CALL ducklake_merge_adjacent_files('t', 'orders');

SELECT 'Compact Worker: Compaction completed, count=' || COUNT(*) as msg FROM orders;
