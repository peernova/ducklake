-- Setup: Partitioned orders table with status as partition key
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_partition_migration host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_partition_migration');
USE t;

-- Create orders table partitioned by status
CREATE TABLE orders (
    id INT,
    customer VARCHAR,
    amount DECIMAL(10,2),
    status VARCHAR
);

ALTER TABLE orders SET PARTITIONED BY (status);

-- Insert orders in different statuses
INSERT INTO orders VALUES
    (1, 'Customer_A', 100.00, 'pending'),
    (2, 'Customer_B', 200.00, 'pending'),
    (3, 'Customer_C', 300.00, 'pending'),
    (4, 'Customer_D', 400.00, 'processing'),
    (5, 'Customer_E', 500.00, 'processing'),
    (6, 'Customer_F', 600.00, 'shipped'),
    (7, 'Customer_G', 700.00, 'shipped'),
    (8, 'Customer_H', 800.00, 'delivered');

SELECT 'Main: Created orders with ' || COUNT(*)::VARCHAR || ' rows' FROM orders;
SELECT status, COUNT(*) as cnt FROM orders GROUP BY status ORDER BY status;

-- Create 4 branches for migration scenarios
CALL ducklake_create_branch('t', 'migrate_a');
CALL ducklake_create_branch('t', 'migrate_b');
CALL ducklake_create_branch('t', 'migrate_c');
CALL ducklake_create_branch('t', 'migrate_d');

SELECT 'Created 4 migration branches' as msg;
