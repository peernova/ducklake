-- Worker 3: Insert 5 rows on main branch
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=same_branch_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/same_branch_test');
USE t;

SELECT 'W3: Starting inserts...';
INSERT INTO counter VALUES (301, 1, 'worker3');
INSERT INTO counter VALUES (302, 2, 'worker3');
INSERT INTO counter VALUES (303, 3, 'worker3');
INSERT INTO counter VALUES (304, 4, 'worker3');
INSERT INTO counter VALUES (305, 5, 'worker3');
SELECT 'W3: Done - inserted 5 rows';
