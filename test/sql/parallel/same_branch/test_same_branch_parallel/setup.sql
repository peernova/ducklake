-- Setup: Create counter table on main branch for parallel INSERT test
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=same_branch_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/same_branch_test');
USE t;

-- Create table with initial row
CREATE TABLE counter (id INT, value INT, worker VARCHAR);
INSERT INTO counter VALUES (0, 0, 'setup');

SELECT 'Setup complete - 1 row';
SELECT * FROM counter;
