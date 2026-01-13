-- Worker A: Runs compaction on test_branch while Worker B inserts on the SAME branch
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=parallel_compact host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_compact');
USE t;
CALL ducklake_use_branch('t', 'test_branch');

SELECT 'Worker A: Starting compaction on test_branch' as msg;
SELECT 'Worker A: Initial count: ' || COUNT(*) as msg FROM orders;

-- Small delay to let Worker B start
SELECT pg_sleep(0.3);

-- Run compaction - this should not interfere with concurrent inserts
SELECT 'Worker A: Running compaction...' as msg;
CALL ducklake_merge_adjacent_files('t', 'orders');

SELECT 'Worker A: After compaction: ' || COUNT(*) as msg FROM orders;
SELECT 'Worker A: Compaction complete' as msg;
