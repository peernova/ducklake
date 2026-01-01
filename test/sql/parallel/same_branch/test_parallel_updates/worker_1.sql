-- Worker 1: Update row id=1 multiple times
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=same_branch_updates host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/same_branch_updates');
USE t;

SELECT 'W1: Updating row id=1...';
UPDATE counter SET value = value + 10, updated_by = 'worker1' WHERE id = 1;
UPDATE counter SET value = value + 10 WHERE id = 1;
UPDATE counter SET value = value + 10 WHERE id = 1;
UPDATE counter SET value = value + 10 WHERE id = 1;
UPDATE counter SET value = value + 10 WHERE id = 1;
SELECT 'W1: Done - added 50 to row 1';
