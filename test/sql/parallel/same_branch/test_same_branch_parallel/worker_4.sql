-- Worker 4: Insert 5 rows on main branch
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=same_branch_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/same_branch_test');
USE t;

SELECT 'W4: Starting inserts...';
INSERT INTO counter VALUES (401, 1, 'worker4');
INSERT INTO counter VALUES (402, 2, 'worker4');
INSERT INTO counter VALUES (403, 3, 'worker4');
INSERT INTO counter VALUES (404, 4, 'worker4');
INSERT INTO counter VALUES (405, 5, 'worker4');
SELECT 'W4: Done - inserted 5 rows';
