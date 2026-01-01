-- Setup: Data table with 100 rows for bulk ops
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_bulk host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_bulk');
USE t;

CREATE TABLE data (
    id INT,
    category VARCHAR,
    value INT,
    status VARCHAR
);

-- Insert 100 rows
INSERT INTO data
SELECT
    i as id,
    CASE i % 5 WHEN 0 THEN 'A' WHEN 1 THEN 'B' WHEN 2 THEN 'C' WHEN 3 THEN 'D' ELSE 'E' END as category,
    i * 10 as value,
    'active' as status
FROM generate_series(1, 100) as t(i);

SELECT 'Main: Created data with ' || COUNT(*)::VARCHAR || ' rows, sum=' || SUM(value)::VARCHAR FROM data;

CALL ducklake_create_branch('t', 'bulk_insert');
CALL ducklake_create_branch('t', 'bulk_delete');
CALL ducklake_create_branch('t', 'bulk_update');
CALL ducklake_create_branch('t', 'bulk_mixed');

SELECT 'Created 4 branches for bulk operations' as msg;
