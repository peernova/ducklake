-- Worker C: Updates row 1 name to 'UPDATED_C'
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_same_row host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_same_row');
USE t;

CALL ducklake_use_branch('t', 'branch_c');
SELECT * FROM ducklake_current_branch('t');

SELECT 'BRANCH_C: Updating row 1 name to UPDATED_C';
UPDATE accounts SET name = 'UPDATED_C' WHERE id = 1;

SELECT 'BRANCH_C: Also updating row 2 name';
UPDATE accounts SET name = 'ALSO_C' WHERE id = 2;

SELECT 'BRANCH_C: Final state:';
SELECT * FROM accounts ORDER BY id;
