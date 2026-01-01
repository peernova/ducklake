-- Chaos Stress Test Setup
-- Creates initial tables with data that workers will hammer on
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=chaos_stress_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/chaos_stress_test');
USE t;

-- Table 1: Orders (partitioned by status)
CREATE TABLE orders (
    id INTEGER,
    customer_id INTEGER,
    amount DECIMAL(10,2),
    status VARCHAR,
    region VARCHAR,
    created_at TIMESTAMP
);
ALTER TABLE orders SET PARTITIONED BY (status);

INSERT INTO orders VALUES
    (1, 100, 150.00, 'pending', 'US', '2024-01-01 10:00:00'),
    (2, 101, 250.00, 'pending', 'EU', '2024-01-01 11:00:00'),
    (3, 102, 350.00, 'completed', 'US', '2024-01-01 12:00:00'),
    (4, 103, 450.00, 'completed', 'ASIA', '2024-01-01 13:00:00'),
    (5, 104, 550.00, 'cancelled', 'EU', '2024-01-01 14:00:00'),
    (6, 105, 650.00, 'pending', 'US', '2024-01-01 15:00:00'),
    (7, 106, 750.00, 'completed', 'ASIA', '2024-01-01 16:00:00'),
    (8, 107, 850.00, 'pending', 'EU', '2024-01-01 17:00:00'),
    (9, 108, 950.00, 'cancelled', 'US', '2024-01-01 18:00:00'),
    (10, 109, 1050.00, 'completed', 'ASIA', '2024-01-01 19:00:00');

-- Table 2: Products (partitioned by category)
CREATE TABLE products (
    id INTEGER,
    name VARCHAR,
    price DECIMAL(10,2),
    category VARCHAR,
    stock INTEGER
);
ALTER TABLE products SET PARTITIONED BY (category);

INSERT INTO products VALUES
    (1, 'Laptop', 999.99, 'Electronics', 50),
    (2, 'Mouse', 29.99, 'Electronics', 200),
    (3, 'Desk', 249.99, 'Furniture', 30),
    (4, 'Chair', 199.99, 'Furniture', 45),
    (5, 'Notebook', 4.99, 'Office', 500),
    (6, 'Pen', 1.99, 'Office', 1000);

-- Table 3: Users (not partitioned)
CREATE TABLE users (
    id INTEGER,
    name VARCHAR,
    email VARCHAR,
    role VARCHAR
);

INSERT INTO users VALUES
    (1, 'Alice', 'alice@test.com', 'admin'),
    (2, 'Bob', 'bob@test.com', 'user'),
    (3, 'Charlie', 'charlie@test.com', 'user'),
    (4, 'Diana', 'diana@test.com', 'manager');

-- Create 4 branches for workers
CALL ducklake_create_branch('t', 'worker_1_branch');
CALL ducklake_create_branch('t', 'worker_2_branch');
CALL ducklake_create_branch('t', 'worker_3_branch');
CALL ducklake_create_branch('t', 'worker_4_branch');

SELECT 'Setup complete: 3 tables, 4 branches created';
SELECT 'orders: 10 rows (partitioned by status)';
SELECT 'products: 6 rows (partitioned by category)';
SELECT 'users: 4 rows (not partitioned)';
