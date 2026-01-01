-- Worker D: 5 sequential inserts on branch_d
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=race_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/race_test');
USE t;
CALL ducklake_use_branch('t', 'branch_d');

SELECT 'D: Starting...';
INSERT INTO counter VALUES (40, 'branch_d', 1);
INSERT INTO counter VALUES (41, 'branch_d', 2);
INSERT INTO counter VALUES (42, 'branch_d', 3);
INSERT INTO counter VALUES (43, 'branch_d', 4);
INSERT INTO counter VALUES (44, 'branch_d', 5);
SELECT 'D: Inserted 5 rows';
SELECT 'D: Count = ' || COUNT(*)::VARCHAR FROM counter;
SELECT * FROM counter ORDER BY id;
