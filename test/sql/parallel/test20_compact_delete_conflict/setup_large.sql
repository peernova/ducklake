-- Setup: Create table with MORE data and MORE files for longer compaction
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=compact_delete_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/compact_delete_test');
USE t;

-- Create table with many small files for longer compaction
CREATE TABLE orders(id INT, customer VARCHAR, amount DECIMAL, data VARCHAR);

-- Insert many batches to create many files
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, repeat('x', 100) FROM range(1, 5001) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, repeat('x', 100) FROM range(5001, 10001) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, repeat('x', 100) FROM range(10001, 15001) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, repeat('x', 100) FROM range(15001, 20001) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, repeat('x', 100) FROM range(20001, 25001) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, repeat('x', 100) FROM range(25001, 30001) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, repeat('x', 100) FROM range(30001, 35001) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, repeat('x', 100) FROM range(35001, 40001) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, repeat('x', 100) FROM range(40001, 45001) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, repeat('x', 100) FROM range(45001, 50001) r(i);

SELECT 'Setup: Created table with ' || COUNT(*) || ' rows in many files' as msg FROM orders;
