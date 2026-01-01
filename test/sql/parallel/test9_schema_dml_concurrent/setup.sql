-- Setup: Products table for concurrent schema/DML test
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_schema_dml host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_schema_dml');
USE t;

-- Create products table
CREATE TABLE products (
    id INT,
    name VARCHAR,
    price DECIMAL(10,2),
    category VARCHAR
);

-- Insert initial data
INSERT INTO products VALUES
    (1, 'Laptop', 999.99, 'electronics'),
    (2, 'Phone', 699.99, 'electronics'),
    (3, 'Tablet', 499.99, 'electronics'),
    (4, 'Desk', 299.99, 'furniture'),
    (5, 'Chair', 199.99, 'furniture'),
    (6, 'Monitor', 399.99, 'electronics'),
    (7, 'Keyboard', 79.99, 'accessories'),
    (8, 'Mouse', 49.99, 'accessories'),
    (9, 'Headphones', 149.99, 'accessories'),
    (10, 'Webcam', 89.99, 'electronics');

SELECT 'Main: Created products with ' || COUNT(*)::VARCHAR || ' rows' FROM products;

-- Create 4 branches
CALL ducklake_create_branch('t', 'schema_worker');
CALL ducklake_create_branch('t', 'dml_heavy');
CALL ducklake_create_branch('t', 'dml_updates');
CALL ducklake_create_branch('t', 'dml_deletes');

SELECT 'Created 4 branches for concurrent schema/DML test' as msg;
