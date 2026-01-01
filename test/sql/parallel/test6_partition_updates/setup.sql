-- Setup: Partitioned sales table with regional data
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_partition_updates host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_partition_updates');
USE t;

-- Create partitioned sales table
CREATE TABLE sales (
    id INT,
    region VARCHAR,
    product VARCHAR,
    amount DECIMAL(10,2),
    quantity INT
);

ALTER TABLE sales SET PARTITIONED BY (region);

-- Insert initial data across regions
INSERT INTO sales VALUES
    (1, 'US', 'Laptop', 1000.00, 10),
    (2, 'US', 'Phone', 500.00, 20),
    (3, 'US', 'Tablet', 300.00, 15),
    (4, 'EU', 'Laptop', 1100.00, 8),
    (5, 'EU', 'Phone', 550.00, 18),
    (6, 'EU', 'Tablet', 330.00, 12),
    (7, 'ASIA', 'Laptop', 900.00, 25),
    (8, 'ASIA', 'Phone', 450.00, 30),
    (9, 'ASIA', 'Tablet', 270.00, 20);

SELECT 'Main: Created sales with ' || COUNT(*)::VARCHAR || ' rows' FROM sales;
SELECT region, COUNT(*) as cnt, SUM(amount)::VARCHAR as total FROM sales GROUP BY region ORDER BY region;

-- Create 4 regional branches
CALL ducklake_create_branch('t', 'us_team');
CALL ducklake_create_branch('t', 'eu_team');
CALL ducklake_create_branch('t', 'asia_team');
CALL ducklake_create_branch('t', 'global_team');

SELECT 'Created 4 regional team branches' as msg;
