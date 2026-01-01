-- Setup: Create partitioned table with data on main, then branches with modifications
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=partition_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/partition_test');
USE t;

-- Create sales table first
CREATE TABLE sales (
    id INTEGER,
    product VARCHAR,
    amount DECIMAL(10,2),
    region VARCHAR,
    sale_date DATE
);

-- Then set partitioning
ALTER TABLE sales SET PARTITIONED BY (region);

-- Insert initial data across regions
INSERT INTO sales VALUES
    (1, 'Laptop', 999.99, 'US', '2024-01-15'),
    (2, 'Mouse', 29.99, 'US', '2024-01-16'),
    (3, 'Keyboard', 79.99, 'EU', '2024-01-15'),
    (4, 'Monitor', 399.99, 'EU', '2024-01-17'),
    (5, 'Desk', 249.99, 'ASIA', '2024-01-16'),
    (6, 'Chair', 199.99, 'ASIA', '2024-01-18'),
    (7, 'Lamp', 49.99, 'US', '2024-01-19'),
    (8, 'Notebook', 4.99, 'EU', '2024-01-20'),
    (9, 'Pen Set', 12.99, 'ASIA', '2024-01-21');

SELECT 'Main branch: 9 sales across 3 regions (US, EU, ASIA)';
SELECT region, COUNT(*) as count, SUM(amount) as total FROM sales GROUP BY region ORDER BY region;

-- Create branch for US operations
CALL ducklake_create_branch('t', 'us_branch');
CALL ducklake_use_branch('t', 'us_branch');
INSERT INTO sales VALUES
    (10, 'Webcam', 89.99, 'US', '2024-01-22'),
    (11, 'Headphones', 149.99, 'US', '2024-01-23');
SELECT 'us_branch: Added 2 more US sales (5 total US)';

-- Create branch for EU operations
CALL ducklake_use_branch('t', 'main');
CALL ducklake_create_branch('t', 'eu_branch');
CALL ducklake_use_branch('t', 'eu_branch');
INSERT INTO sales VALUES
    (12, 'Tablet', 599.99, 'EU', '2024-01-22'),
    (13, 'Phone', 899.99, 'EU', '2024-01-23'),
    (14, 'Watch', 299.99, 'EU', '2024-01-24');
SELECT 'eu_branch: Added 3 more EU sales (5 total EU)';

-- Create branch for ASIA operations with price adjustments
CALL ducklake_use_branch('t', 'main');
CALL ducklake_create_branch('t', 'asia_branch');
CALL ducklake_use_branch('t', 'asia_branch');
UPDATE sales SET amount = amount * 1.1 WHERE region = 'ASIA';
SELECT 'asia_branch: Increased ASIA prices by 10%';

-- Switch back to main
CALL ducklake_use_branch('t', 'main');
SELECT 'Setup complete. Branches: main, us_branch, eu_branch, asia_branch';
