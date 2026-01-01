-- Worker 3: Heavy UPDATE operations
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_stress host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_stress');
USE t;
CALL ducklake_use_branch('t', 'worker_3');

SELECT 'W3: Starting heavy UPDATEs...';
UPDATE transactions SET amount = amount * 2 WHERE type = 'deposit';
SELECT 'W3: Doubled all deposits';
UPDATE transactions SET account = 'W3_' || account;
SELECT 'W3: Prefixed all accounts';
SELECT 'W3: Total = ' || SUM(amount)::VARCHAR FROM transactions;
