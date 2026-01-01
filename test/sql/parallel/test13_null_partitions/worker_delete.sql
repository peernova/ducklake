-- Delete Nulls: Removes all 'uncategorized' rows
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_null_part host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_null_part');
USE t;

CALL ducklake_use_branch('t', 'delete_nulls');
SELECT 'DELETE: Removing uncategorized rows...';

DELETE FROM logs WHERE category = 'uncategorized';

SELECT 'DELETE: Final state (only categorized):';
SELECT category, COUNT(*) as cnt FROM logs GROUP BY category ORDER BY category;
SELECT 'DELETE: Total rows = ' || COUNT(*)::VARCHAR FROM logs;
