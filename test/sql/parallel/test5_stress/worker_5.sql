-- Worker 5: Mixed operations with cross-branch queries
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_stress host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_stress');
USE t;
CALL ducklake_use_branch('t', 'worker_5');

SELECT 'W5: Starting mixed operations with cross-queries...';
INSERT INTO transactions VALUES (501, 'W5_ACC', 111.00, 'deposit');
SELECT 'W5: Main total = ' || SUM(amount)::VARCHAR FROM transactions AT (BRANCH => 'main');
DELETE FROM transactions WHERE id = 4;
SELECT 'W5: Deleted withdrawal id=4';
INSERT INTO transactions VALUES (502, 'W5_ACC', 222.00, 'deposit');
SELECT 'W5: Worker_1 total = ' || SUM(amount)::VARCHAR FROM transactions AT (BRANCH => 'worker_1');
UPDATE transactions SET type = 'verified_deposit' WHERE type = 'deposit';
SELECT 'W5: Updated deposit types';
SELECT 'W5: Total = ' || SUM(amount)::VARCHAR FROM transactions;
