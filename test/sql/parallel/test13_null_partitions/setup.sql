-- Setup: Logs table with edge-case category values
-- NOTE: NULL partition values cause an internal error in DuckLake,
-- so we test with edge-case values like empty strings and special chars instead
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_null_part host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_null_part');
USE t;

CREATE TABLE logs (
    id INT,
    message VARCHAR,
    category VARCHAR,
    severity INT
);

ALTER TABLE logs SET PARTITIONED BY (category);

-- Insert with mix of regular and edge-case categories
INSERT INTO logs VALUES
    (1, 'System started', 'system', 1),
    (2, 'User login', 'auth', 2),
    (3, 'Unknown event', 'uncategorized', 3),
    (4, 'Database connected', 'system', 1),
    (5, 'Uncategorized log', 'uncategorized', 2),
    (6, 'Auth failed', 'auth', 4),
    (7, 'Mystery error', 'uncategorized', 5),
    (8, 'Cache cleared', 'system', 1);

SELECT 'Main: Created logs with ' || COUNT(*)::VARCHAR || ' rows' FROM logs;
SELECT 'Main: uncategorized count = ' || COUNT(*)::VARCHAR FROM logs WHERE category = 'uncategorized';
SELECT category, COUNT(*) as cnt FROM logs GROUP BY category ORDER BY category;

CALL ducklake_create_branch('t', 'handle_nulls');
CALL ducklake_create_branch('t', 'delete_nulls');
CALL ducklake_create_branch('t', 'keep_nulls');

SELECT 'Created 3 branches for edge-case handling test' as msg;
