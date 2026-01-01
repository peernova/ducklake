-- Worker C: 5 sequential inserts on branch_c
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=race_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/race_test');
USE t;
CALL ducklake_use_branch('t', 'branch_c');

SELECT 'C: Starting...';
INSERT INTO counter VALUES (30, 'branch_c', 1);
INSERT INTO counter VALUES (31, 'branch_c', 2);
INSERT INTO counter VALUES (32, 'branch_c', 3);
INSERT INTO counter VALUES (33, 'branch_c', 4);
INSERT INTO counter VALUES (34, 'branch_c', 5);
SELECT 'C: Inserted 5 rows';
SELECT 'C: Count = ' || COUNT(*)::VARCHAR FROM counter;
SELECT * FROM counter ORDER BY id;
