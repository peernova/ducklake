-- Setup: Create tables for parallel MERGE testing
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=parallel_merge host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_merge');
USE t;

-- Target table: products
CREATE TABLE products (
    product_id INT,
    name VARCHAR,
    price DECIMAL(10,2),
    stock INT
);

-- Insert initial data
INSERT INTO products VALUES
    (1, 'Widget A', 10.00, 100),
    (2, 'Widget B', 20.00, 200),
    (3, 'Widget C', 30.00, 300),
    (4, 'Widget D', 40.00, 400),
    (5, 'Widget E', 50.00, 500);

SELECT 'Setup: Initial products:' as msg;
SELECT * FROM products ORDER BY product_id;

-- Create branches for parallel MERGE testing
CALL ducklake_create_branch('t', 'branch_a');
CALL ducklake_create_branch('t', 'branch_b');
CALL ducklake_create_branch('t', 'branch_c');

SELECT 'Created 3 branches for parallel MERGE testing' as msg;
