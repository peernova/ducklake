-- Worker A: Sets value to 100 (unique to branch_a)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=race_same_row host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/race_same_row');
USE t;
CALL ducklake_use_branch('t', 'branch_a');

SELECT 'A: Setting value to 100...';
UPDATE counter SET value = 100, branch_name = 'branch_a' WHERE id = 1;
SELECT 'A: Done. Value = ' || value::VARCHAR FROM counter WHERE id = 1;
