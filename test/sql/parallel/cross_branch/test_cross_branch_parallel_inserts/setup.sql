-- Race Condition Test: Verify branch isolation under parallel commits
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=race_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/race_test');
USE t;

-- Simple table with counter
CREATE TABLE counter (id INT, branch_name VARCHAR, value INT);
INSERT INTO counter VALUES (1, 'main', 0);
SELECT 'Main: Created counter table';

-- Create 4 branches - each will increment independently
CALL ducklake_create_branch('t', 'branch_a');
CALL ducklake_create_branch('t', 'branch_b');
CALL ducklake_create_branch('t', 'branch_c');
CALL ducklake_create_branch('t', 'branch_d');
SELECT 'Created 4 branches for race condition test';
