-- Setup: Create table with data for compact vs delete conflict test
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=compact_delete_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/compact_delete_test');
USE t;

-- Create table with multiple small files for compaction
CREATE TABLE orders(id INT, customer VARCHAR, amount DECIMAL);

-- Insert multiple batches to create multiple files
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0 FROM range(1, 501) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0 FROM range(501, 1001) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0 FROM range(1001, 1501) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0 FROM range(1501, 2001) r(i);

SELECT 'Setup: Created table with ' || COUNT(*) || ' rows' as msg FROM orders;
