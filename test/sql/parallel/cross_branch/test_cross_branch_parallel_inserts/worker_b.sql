-- Worker B: 5 sequential inserts on branch_b
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=race_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/race_test');
USE t;
CALL ducklake_use_branch('t', 'branch_b');

SELECT 'B: Starting...';
INSERT INTO counter VALUES (20, 'branch_b', 1);
INSERT INTO counter VALUES (21, 'branch_b', 2);
INSERT INTO counter VALUES (22, 'branch_b', 3);
INSERT INTO counter VALUES (23, 'branch_b', 4);
INSERT INTO counter VALUES (24, 'branch_b', 5);
SELECT 'B: Inserted 5 rows';
SELECT 'B: Count = ' || COUNT(*)::VARCHAR FROM counter;
SELECT * FROM counter ORDER BY id;
