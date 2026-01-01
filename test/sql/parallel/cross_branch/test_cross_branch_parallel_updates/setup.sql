-- Setup: Create counter table and 4 branches for UPDATE race test
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=race_update_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/race_update_test');
USE t;

-- Create table with initial rows - each branch will UPDATE its own row
CREATE TABLE counter (id INT, value INT, branch_name VARCHAR);
INSERT INTO counter VALUES (1, 100, 'main');
INSERT INTO counter VALUES (2, 100, 'main');
INSERT INTO counter VALUES (3, 100, 'main');
INSERT INTO counter VALUES (4, 100, 'main');

SELECT 'Main setup complete - 4 rows with value=100 each';
SELECT * FROM counter ORDER BY id;

-- Create 4 branches
CALL ducklake_create_branch('t', 'branch_a');
CALL ducklake_create_branch('t', 'branch_b');
CALL ducklake_create_branch('t', 'branch_c');
CALL ducklake_create_branch('t', 'branch_d');

SELECT 'Created 4 branches';
