-- Worker A: 5 sequential inserts on branch_a
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=race_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/race_test');
USE t;
CALL ducklake_use_branch('t', 'branch_a');

SELECT 'A: Starting...';
INSERT INTO counter VALUES (10, 'branch_a', 1);
INSERT INTO counter VALUES (11, 'branch_a', 2);
INSERT INTO counter VALUES (12, 'branch_a', 3);
INSERT INTO counter VALUES (13, 'branch_a', 4);
INSERT INTO counter VALUES (14, 'branch_a', 5);
SELECT 'A: Inserted 5 rows';
SELECT 'A: Count = ' || COUNT(*)::VARCHAR FROM counter;
SELECT * FROM counter ORDER BY id;
