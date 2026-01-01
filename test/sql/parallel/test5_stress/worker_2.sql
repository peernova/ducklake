-- Worker 2: Heavy DELETE operations
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_stress host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_stress');
USE t;
CALL ducklake_use_branch('t', 'worker_2');

SELECT 'W2: Starting heavy DELETEs...';
DELETE FROM transactions WHERE id = 1;
DELETE FROM transactions WHERE id = 2;
DELETE FROM transactions WHERE id = 3;
SELECT 'W2: Deleted 3 transactions';
INSERT INTO transactions VALUES (201, 'W2_ACC', 9999.00, 'bonus');
SELECT 'W2: Added bonus transaction';
SELECT 'W2: Total = ' || SUM(amount)::VARCHAR FROM transactions;
