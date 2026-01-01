-- Setup: Create counter table and 4 branches
-- All branches will UPDATE the SAME row to test true isolation
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=race_same_row host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/race_same_row');
USE t;

-- Create table with single row that all branches will update
CREATE TABLE counter (id INT, value INT, branch_name VARCHAR);
INSERT INTO counter VALUES (1, 0, 'main');

SELECT 'Main setup complete - 1 row with value=0';
SELECT * FROM counter;

-- Create 4 branches from main (all starting with value=0)
CALL ducklake_create_branch('t', 'branch_a');
CALL ducklake_create_branch('t', 'branch_b');
CALL ducklake_create_branch('t', 'branch_c');
CALL ducklake_create_branch('t', 'branch_d');

SELECT 'Created 4 branches - all start with value=0';
