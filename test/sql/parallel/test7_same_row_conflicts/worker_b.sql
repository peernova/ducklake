-- Worker B: Deletes row 1 entirely
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_same_row host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_same_row');
USE t;

CALL ducklake_use_branch('t', 'branch_b');
SELECT * FROM ducklake_current_branch('t');

SELECT 'BRANCH_B: Deleting row 1 entirely';
DELETE FROM accounts WHERE id = 1;

SELECT 'BRANCH_B: Row 1 should not exist:';
SELECT COUNT(*) as row1_count FROM accounts WHERE id = 1;

SELECT 'BRANCH_B: All rows (should be 4):';
SELECT * FROM accounts ORDER BY id;
