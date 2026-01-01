-- Worker 1: Delete category='A'
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=same_branch_deletes host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/same_branch_deletes');
USE t;

SELECT 'W1: Deleting category A...';
DELETE FROM data WHERE category = 'A';
SELECT 'W1: Done';
