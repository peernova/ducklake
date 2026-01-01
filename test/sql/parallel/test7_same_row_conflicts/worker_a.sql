-- Worker A: Updates row 1 balance to 5000
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_same_row host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_same_row');
USE t;

CALL ducklake_use_branch('t', 'branch_a');
SELECT * FROM ducklake_current_branch('t');

SELECT 'BRANCH_A: Updating row 1 balance to 5000';
UPDATE accounts SET balance = 5000.00 WHERE id = 1;

SELECT 'BRANCH_A: Also updating row 1 status';
UPDATE accounts SET status = 'premium' WHERE id = 1;

SELECT 'BRANCH_A: Final state of row 1:';
SELECT * FROM accounts WHERE id = 1;

SELECT 'BRANCH_A: All rows:';
SELECT * FROM accounts ORDER BY id;
