-- Worker B: Inserts data on test_branch while Worker A runs compaction on the SAME branch
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=parallel_compact host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_compact');
USE t;
CALL ducklake_use_branch('t', 'test_branch');

SELECT 'Worker B: Starting inserts on test_branch' as msg;
SELECT 'Worker B: Initial count: ' || COUNT(*) as msg FROM orders;

-- Insert data in multiple batches while compaction may be running
INSERT INTO orders SELECT i, 'concurrent_cust1', i * 7.0, 'LATAM' FROM range(3001, 3251) r(i);
SELECT 'Worker B: After insert 1: ' || COUNT(*) as msg FROM orders;

SELECT pg_sleep(0.2);

INSERT INTO orders SELECT i, 'concurrent_cust2', i * 8.0, 'LATAM' FROM range(3251, 3501) r(i);
SELECT 'Worker B: After insert 2: ' || COUNT(*) as msg FROM orders;

SELECT pg_sleep(0.2);

INSERT INTO orders SELECT i, 'concurrent_cust3', i * 9.0, 'AFRICA' FROM range(3501, 3751) r(i);
SELECT 'Worker B: After insert 3: ' || COUNT(*) as msg FROM orders;

SELECT pg_sleep(0.2);

INSERT INTO orders SELECT i, 'concurrent_cust4', i * 10.0, 'AFRICA' FROM range(3751, 4001) r(i);
SELECT 'Worker B: After insert 4: ' || COUNT(*) as msg FROM orders;

SELECT 'Worker B: Final count: ' || COUNT(*) as msg FROM orders;
SELECT 'Worker B: Inserts complete' as msg;
