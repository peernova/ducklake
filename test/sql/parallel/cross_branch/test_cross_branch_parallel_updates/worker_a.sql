-- Worker A: 5 sequential UPDATEs on branch_a (each +10, total +50)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=race_update_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/race_update_test');
USE t;
CALL ducklake_use_branch('t', 'branch_a');

SELECT 'A: Starting UPDATEs on row id=1...';
UPDATE counter SET value = value + 10, branch_name = 'branch_a' WHERE id = 1;
UPDATE counter SET value = value + 10 WHERE id = 1;
UPDATE counter SET value = value + 10 WHERE id = 1;
UPDATE counter SET value = value + 10 WHERE id = 1;
UPDATE counter SET value = value + 10 WHERE id = 1;
SELECT 'A: Done 5 UPDATEs (+50 total)';
SELECT 'A: Value = ' || value::VARCHAR FROM counter WHERE id = 1;
