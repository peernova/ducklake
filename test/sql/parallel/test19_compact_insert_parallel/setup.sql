-- Setup: Create table with many small files on a branch for parallel compaction/insert test
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=parallel_compact host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_compact');
USE t;

-- Create table with initial data - multiple small files for compaction
CREATE TABLE orders(id INT, customer VARCHAR, amount DECIMAL, region VARCHAR);

-- Insert many small batches to create multiple files
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, 'US' FROM range(1, 501) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, 'US' FROM range(501, 1001) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, 'EU' FROM range(1001, 1501) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, 'EU' FROM range(1501, 2001) r(i);

SELECT 'Setup: Main initial count: ' || COUNT(*) as msg FROM orders;

-- Create a branch where we'll do parallel compaction and inserts
CALL ducklake_create_branch('t', 'test_branch');

-- Switch to test_branch and add more small files for compaction
CALL ducklake_use_branch('t', 'test_branch');

INSERT INTO orders SELECT i, 'branch_cust', i * 5.0, 'ASIA' FROM range(2001, 2251) r(i);
INSERT INTO orders SELECT i, 'branch_cust', i * 5.0, 'ASIA' FROM range(2251, 2501) r(i);
INSERT INTO orders SELECT i, 'branch_cust', i * 5.0, 'ASIA' FROM range(2501, 2751) r(i);
INSERT INTO orders SELECT i, 'branch_cust', i * 5.0, 'ASIA' FROM range(2751, 3001) r(i);

SELECT 'Setup: test_branch initial count: ' || COUNT(*) as msg FROM orders;
SELECT 'Setup: Created test_branch with multiple small files' as msg;
