-- Keep Nulls: Keeps uncategorized and adds more
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_null_part host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_null_part');
USE t;

CALL ducklake_use_branch('t', 'keep_nulls');
SELECT 'KEEP: Adding more uncategorized rows...';

INSERT INTO logs VALUES
    (101, 'New unknown 1', 'uncategorized', 2),
    (102, 'New unknown 2', 'uncategorized', 3),
    (103, 'New unknown 3', 'uncategorized', 4);

-- Update uncategorized severity
UPDATE logs SET severity = severity + 1 WHERE category = 'uncategorized';

SELECT 'KEEP: Final state (more uncategorized):';
SELECT category, COUNT(*) as cnt FROM logs GROUP BY category ORDER BY category;
SELECT 'KEEP: uncategorized count = ' || COUNT(*)::VARCHAR FROM logs WHERE category = 'uncategorized';
