-- Handle Nulls: Reclassifies 'uncategorized' rows to proper categories
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_null_part host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_null_part');
USE t;

CALL ducklake_use_branch('t', 'handle_nulls');
SELECT 'HANDLE: Categorizing uncategorized rows...';

-- Assign category based on severity
UPDATE logs SET category = 'error' WHERE category = 'uncategorized' AND severity >= 4;
UPDATE logs SET category = 'warning' WHERE category = 'uncategorized' AND severity = 3;
UPDATE logs SET category = 'info' WHERE category = 'uncategorized';

SELECT 'HANDLE: Final state (no uncategorized):';
SELECT category, COUNT(*) as cnt FROM logs GROUP BY category ORDER BY category;
SELECT 'HANDLE: uncategorized count = ' || COUNT(*)::VARCHAR FROM logs WHERE category = 'uncategorized';
