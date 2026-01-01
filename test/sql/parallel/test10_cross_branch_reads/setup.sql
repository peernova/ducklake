-- Setup: Metrics table for cross-branch read test
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_cross_reads host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_cross_reads');
USE t;

CREATE TABLE metrics (
    id INT,
    source VARCHAR,
    value INT,
    timestamp_val INT
);

-- Initial data on main
INSERT INTO metrics VALUES
    (1, 'main', 100, 0),
    (2, 'main', 200, 0),
    (3, 'main', 300, 0);

SELECT 'Main: Created metrics with ' || COUNT(*)::VARCHAR || ' rows' FROM metrics;

CALL ducklake_create_branch('t', 'writer_a');
CALL ducklake_create_branch('t', 'writer_b');
CALL ducklake_create_branch('t', 'reader_only');

SELECT 'Created 3 branches for cross-read test' as msg;
