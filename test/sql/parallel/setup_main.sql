-- Setup: Run this first to create the table and branches
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_data');
USE t;

CREATE TABLE users (id INT, name VARCHAR, status VARCHAR DEFAULT 'active');
INSERT INTO users VALUES (1, 'Alice', 'active'), (2, 'Bob', 'active'), (3, 'Charlie', 'active');
SELECT 'Main: ' || COUNT(*)::VARCHAR || ' users' FROM users;

CALL ducklake_create_branch('t', 'branch_a');
CALL ducklake_create_branch('t', 'branch_b');
CALL ducklake_create_branch('t', 'branch_c');
SELECT 'Created 3 branches';
