-- Worker 1: Heavy INSERT operations
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_stress host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_stress');
USE t;
CALL ducklake_use_branch('t', 'worker_1');

SELECT 'W1: Starting heavy INSERTs...';
INSERT INTO transactions VALUES (101, 'W1_ACC', 100.00, 'deposit');
INSERT INTO transactions VALUES (102, 'W1_ACC', 200.00, 'deposit');
INSERT INTO transactions VALUES (103, 'W1_ACC', 300.00, 'deposit');
INSERT INTO transactions VALUES (104, 'W1_ACC', 400.00, 'deposit');
INSERT INTO transactions VALUES (105, 'W1_ACC', 500.00, 'deposit');
SELECT 'W1: Inserted 5 transactions';
SELECT 'W1: Total = ' || SUM(amount)::VARCHAR FROM transactions;
