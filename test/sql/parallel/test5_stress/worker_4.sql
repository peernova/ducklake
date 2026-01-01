-- Worker 4: Schema change + data modification
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_stress host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_stress');
USE t;
CALL ducklake_use_branch('t', 'worker_4');

SELECT 'W4: Starting schema change...';
ALTER TABLE transactions ADD COLUMN timestamp TIMESTAMP;
SELECT 'W4: Added timestamp column';
ALTER TABLE transactions ADD COLUMN verified BOOLEAN;
SELECT 'W4: Added verified column';
UPDATE transactions SET verified = true WHERE amount > 0;
SELECT 'W4: Verified positive transactions';
INSERT INTO transactions (id, account, amount, type, timestamp, verified) VALUES (401, 'W4_ACC', 777.00, 'special', '2024-01-15 10:30:00', true);
SELECT 'W4: Added special transaction';
SELECT 'W4: Total = ' || SUM(amount)::VARCHAR FROM transactions;
