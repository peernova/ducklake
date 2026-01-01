-- Worker 2: Insert 5 rows on main branch
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=same_branch_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/same_branch_test');
USE t;

SELECT 'W2: Starting inserts...';
INSERT INTO counter VALUES (201, 1, 'worker2');
INSERT INTO counter VALUES (202, 2, 'worker2');
INSERT INTO counter VALUES (203, 3, 'worker2');
INSERT INTO counter VALUES (204, 4, 'worker2');
INSERT INTO counter VALUES (205, 5, 'worker2');
SELECT 'W2: Done - inserted 5 rows';
