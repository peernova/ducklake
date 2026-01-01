-- Worker B: Sets value to 200 (unique to branch_b)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=race_same_row host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/race_same_row');
USE t;
CALL ducklake_use_branch('t', 'branch_b');

SELECT 'B: Setting value to 200...';
UPDATE counter SET value = 200, branch_name = 'branch_b' WHERE id = 1;
SELECT 'B: Done. Value = ' || value::VARCHAR FROM counter WHERE id = 1;
