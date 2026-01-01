-- Worker 6: Bulk operations
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_stress host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_stress');
USE t;
CALL ducklake_use_branch('t', 'worker_6');

SELECT 'W6: Starting bulk operations...';
DELETE FROM transactions WHERE type = 'withdrawal';
SELECT 'W6: Deleted all withdrawals';
INSERT INTO transactions VALUES
    (601, 'W6_BULK', 10.00, 'micro'),
    (602, 'W6_BULK', 20.00, 'micro'),
    (603, 'W6_BULK', 30.00, 'micro'),
    (604, 'W6_BULK', 40.00, 'micro'),
    (605, 'W6_BULK', 50.00, 'micro'),
    (606, 'W6_BULK', 60.00, 'micro'),
    (607, 'W6_BULK', 70.00, 'micro'),
    (608, 'W6_BULK', 80.00, 'micro'),
    (609, 'W6_BULK', 90.00, 'micro'),
    (610, 'W6_BULK', 100.00, 'micro');
SELECT 'W6: Inserted 10 micro transactions';
SELECT 'W6: Total = ' || SUM(amount)::VARCHAR FROM transactions;
