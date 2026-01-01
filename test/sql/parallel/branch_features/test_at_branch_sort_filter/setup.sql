-- Setup: Create table with data, then create branches with different modifications
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=at_branch_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/at_branch_test');
USE t;

-- Create products table with initial data on main branch
CREATE TABLE products (
    id INTEGER,
    name VARCHAR,
    price DECIMAL(10,2),
    category VARCHAR,
    stock INTEGER
);

-- Insert initial data (10 products)
INSERT INTO products VALUES
    (1, 'Laptop', 999.99, 'Electronics', 50),
    (2, 'Mouse', 29.99, 'Electronics', 200),
    (3, 'Keyboard', 79.99, 'Electronics', 150),
    (4, 'Monitor', 399.99, 'Electronics', 75),
    (5, 'Desk', 249.99, 'Furniture', 30),
    (6, 'Chair', 199.99, 'Furniture', 45),
    (7, 'Lamp', 49.99, 'Furniture', 100),
    (8, 'Notebook', 4.99, 'Office', 500),
    (9, 'Pen Set', 12.99, 'Office', 300),
    (10, 'Stapler', 8.99, 'Office', 200);

SELECT 'Main branch setup complete with 10 products';

-- Create branch_discount: Apply 10% discount to all products
CALL ducklake_create_branch('t', 'branch_discount');
CALL ducklake_use_branch('t', 'branch_discount');
UPDATE products SET price = price * 0.9;
SELECT 'branch_discount: Applied 10% discount to all products';

-- Create branch_restock: Double the stock (from main)
CALL ducklake_use_branch('t', 'main');
CALL ducklake_create_branch('t', 'branch_restock');
CALL ducklake_use_branch('t', 'branch_restock');
UPDATE products SET stock = stock * 2;
SELECT 'branch_restock: Doubled stock for all products';

-- Create branch_new_items: Add new products (from main)
CALL ducklake_use_branch('t', 'main');
CALL ducklake_create_branch('t', 'branch_new_items');
CALL ducklake_use_branch('t', 'branch_new_items');
INSERT INTO products VALUES
    (11, 'Headphones', 149.99, 'Electronics', 80),
    (12, 'Webcam', 89.99, 'Electronics', 60),
    (13, 'Bookshelf', 129.99, 'Furniture', 25);
SELECT 'branch_new_items: Added 3 new products (13 total)';

-- Switch back to main for test scripts
CALL ducklake_use_branch('t', 'main');

SELECT 'Setup complete. Branches: main, branch_discount, branch_restock, branch_new_items';
