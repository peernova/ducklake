-- Worker D: Updates rows 1,2,3 balance +100
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_same_row host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_same_row');
USE t;

CALL ducklake_use_branch('t', 'branch_d');
SELECT * FROM ducklake_current_branch('t');

SELECT 'BRANCH_D: Updating rows 1,2,3 balance +100';
UPDATE accounts SET balance = balance + 100.00 WHERE id IN (1, 2, 3);

SELECT 'BRANCH_D: Final state:';
SELECT * FROM accounts ORDER BY id;
SELECT 'BRANCH_D: Sum of balances = ' || SUM(balance)::VARCHAR FROM accounts;
