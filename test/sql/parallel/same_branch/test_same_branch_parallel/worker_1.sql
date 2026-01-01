-- Worker 1: Insert 5 rows on main branch
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=same_branch_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/same_branch_test');
USE t;

SELECT 'W1: Starting inserts...';
INSERT INTO counter VALUES (101, 1, 'worker1');
INSERT INTO counter VALUES (102, 2, 'worker1');
INSERT INTO counter VALUES (103, 3, 'worker1');
INSERT INTO counter VALUES (104, 4, 'worker1');
INSERT INTO counter VALUES (105, 5, 'worker1');
SELECT 'W1: Done - inserted 5 rows';
