-- Test 5: High Concurrency Stress Test (6 workers)
-- Setup: Create data and 6 branches for stress testing
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_stress host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_stress');
USE t;

-- Create transactions table
CREATE TABLE transactions (id INT, account VARCHAR, amount DECIMAL(10,2), type VARCHAR);
INSERT INTO transactions VALUES
    (1, 'ACC001', 1000.00, 'deposit'),
    (2, 'ACC002', 500.00, 'deposit'),
    (3, 'ACC003', 750.00, 'deposit'),
    (4, 'ACC001', -200.00, 'withdrawal'),
    (5, 'ACC002', -100.00, 'withdrawal');
SELECT 'Main: Created transactions with ' || COUNT(*)::VARCHAR || ' rows' FROM transactions;

-- Create 6 branches for high concurrency
CALL ducklake_create_branch('t', 'worker_1');
CALL ducklake_create_branch('t', 'worker_2');
CALL ducklake_create_branch('t', 'worker_3');
CALL ducklake_create_branch('t', 'worker_4');
CALL ducklake_create_branch('t', 'worker_5');
CALL ducklake_create_branch('t', 'worker_6');
SELECT 'Created 6 worker branches for stress test';
