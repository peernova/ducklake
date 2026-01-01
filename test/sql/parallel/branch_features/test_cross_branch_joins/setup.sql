-- Cross-Branch Joins and Multi-Catalog Test Setup
-- Tests joining tables across different branches and catalogs
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

-- Create first catalog (sales system)
ATTACH 'ducklake:postgres:dbname=catalog_sales host=localhost port=5433 user=postgres password=postgres' AS sales (DATA_PATH '/tmp/catalog_sales');

-- Create second catalog (inventory system)
ATTACH 'ducklake:postgres:dbname=catalog_inventory host=localhost port=5433 user=postgres password=postgres' AS inventory (DATA_PATH '/tmp/catalog_inventory');

-- ========== SALES CATALOG ==========
USE sales;

-- Create customers table
CREATE TABLE customers (
    id INTEGER,
    name VARCHAR,
    region VARCHAR,
    tier VARCHAR
);

INSERT INTO customers VALUES
    (1, 'Acme Corp', 'US', 'gold'),
    (2, 'TechStart', 'EU', 'silver'),
    (3, 'GlobalTrade', 'ASIA', 'gold'),
    (4, 'LocalShop', 'US', 'bronze'),
    (5, 'MegaCorp', 'EU', 'platinum');

-- Create orders table
CREATE TABLE orders (
    id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    total DECIMAL(10,2),
    status VARCHAR
);

INSERT INTO orders VALUES
    (1, 1, 101, 10, 1000.00, 'completed'),
    (2, 1, 102, 5, 500.00, 'completed'),
    (3, 2, 101, 3, 300.00, 'pending'),
    (4, 3, 103, 20, 2000.00, 'completed'),
    (5, 4, 102, 2, 200.00, 'cancelled'),
    (6, 5, 101, 50, 5000.00, 'completed'),
    (7, 5, 103, 30, 3000.00, 'pending');

SELECT 'Sales catalog: customers (5) and orders (7) created on main';

-- Create branch with different customer tiers
CALL ducklake_create_branch('sales', 'promo_branch');
CALL ducklake_use_branch('sales', 'promo_branch');
UPDATE customers SET tier = 'platinum' WHERE tier = 'gold';
INSERT INTO orders VALUES (8, 1, 104, 100, 10000.00, 'pending');
SELECT 'Sales promo_branch: upgraded gold->platinum, added big order';

-- Create branch with order cancellations
CALL ducklake_use_branch('sales', 'main');
CALL ducklake_create_branch('sales', 'cleanup_branch');
CALL ducklake_use_branch('sales', 'cleanup_branch');
DELETE FROM orders WHERE status = 'cancelled';
UPDATE orders SET status = 'archived' WHERE status = 'completed';
SELECT 'Sales cleanup_branch: removed cancelled, archived completed';

CALL ducklake_use_branch('sales', 'main');

-- ========== INVENTORY CATALOG ==========
USE inventory;

-- Create products table
CREATE TABLE products (
    id INTEGER,
    name VARCHAR,
    category VARCHAR,
    price DECIMAL(10,2)
);

INSERT INTO products VALUES
    (101, 'Laptop Pro', 'Electronics', 999.99),
    (102, 'Wireless Mouse', 'Electronics', 49.99),
    (103, 'Office Chair', 'Furniture', 299.99),
    (104, 'Standing Desk', 'Furniture', 599.99);

-- Create stock table
CREATE TABLE stock (
    product_id INTEGER,
    warehouse VARCHAR,
    quantity INTEGER
);

INSERT INTO stock VALUES
    (101, 'NYC', 100),
    (101, 'LA', 50),
    (102, 'NYC', 500),
    (102, 'LA', 300),
    (103, 'NYC', 75),
    (103, 'CHI', 60),
    (104, 'LA', 25);

SELECT 'Inventory catalog: products (4) and stock (7) created on main';

-- Create branch with price changes
CALL ducklake_create_branch('inventory', 'sale_branch');
CALL ducklake_use_branch('inventory', 'sale_branch');
UPDATE products SET price = price * 0.8 WHERE category = 'Electronics';
SELECT 'Inventory sale_branch: 20% off Electronics';

-- Create branch with new products
CALL ducklake_use_branch('inventory', 'main');
CALL ducklake_create_branch('inventory', 'new_products_branch');
CALL ducklake_use_branch('inventory', 'new_products_branch');
INSERT INTO products VALUES
    (105, 'Monitor 4K', 'Electronics', 449.99),
    (106, 'Keyboard RGB', 'Electronics', 129.99);
INSERT INTO stock VALUES
    (105, 'NYC', 40),
    (106, 'NYC', 200);
SELECT 'Inventory new_products_branch: added 2 new products';

CALL ducklake_use_branch('inventory', 'main');

SELECT 'Setup complete:';
SELECT '  Sales catalog: main, promo_branch, cleanup_branch';
SELECT '  Inventory catalog: main, sale_branch, new_products_branch';
