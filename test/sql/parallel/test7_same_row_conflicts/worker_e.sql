-- Worker E: Deletes rows 1,2 and updates row 3
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_same_row host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_same_row');
USE t;

CALL ducklake_use_branch('t', 'branch_e');
SELECT * FROM ducklake_current_branch('t');

SELECT 'BRANCH_E: Deleting rows 1 and 2';
DELETE FROM accounts WHERE id IN (1, 2);

SELECT 'BRANCH_E: Updating row 3 to closed status';
UPDATE accounts SET status = 'closed', balance = 0.00 WHERE id = 3;

SELECT 'BRANCH_E: Final state (should be 3 rows):';
SELECT * FROM accounts ORDER BY id;
