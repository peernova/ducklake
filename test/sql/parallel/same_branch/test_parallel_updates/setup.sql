-- Setup: Create table with 4 rows, each worker will UPDATE a different row
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=same_branch_updates host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/same_branch_updates');
USE t;

CREATE TABLE counter (id INT, value INT, updated_by VARCHAR);
INSERT INTO counter VALUES (1, 0, 'setup');
INSERT INTO counter VALUES (2, 0, 'setup');
INSERT INTO counter VALUES (3, 0, 'setup');
INSERT INTO counter VALUES (4, 0, 'setup');

SELECT 'Setup complete - 4 rows with value=0';
SELECT * FROM counter ORDER BY id;
