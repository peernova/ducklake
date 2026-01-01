-- Worker C: Sets value to 300 (unique to branch_c)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=race_same_row host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/race_same_row');
USE t;
CALL ducklake_use_branch('t', 'branch_c');

SELECT 'C: Setting value to 300...';
UPDATE counter SET value = 300, branch_name = 'branch_c' WHERE id = 1;
SELECT 'C: Done. Value = ' || value::VARCHAR FROM counter WHERE id = 1;
