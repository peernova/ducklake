-- Worker 2: Heavy on inserts and updates across tables
-- ~12-15 operations with random delays
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=chaos_stress_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/chaos_stress_test');
USE t;
CALL ducklake_use_branch('t', 'worker_2_branch');

SELECT 'W2: Starting chaos operations...';

-- Delay to mix up timing

-- Op 1: Bulk insert into orders
SELECT 'W2-01: Bulk inserting orders';
INSERT INTO orders VALUES
    (21, 300, 175.00, 'pending', 'EU', '2024-01-03 10:00:00'),
    (22, 301, 275.00, 'pending', 'US', '2024-01-03 11:00:00'),
    (23, 302, 375.00, 'completed', 'ASIA', '2024-01-03 12:00:00'),
    (24, 303, 475.00, 'cancelled', 'EU', '2024-01-03 13:00:00');

-- Op 2: Update all pending orders
SELECT 'W2-02: Increasing pending order amounts';
UPDATE orders SET amount = amount + 25 WHERE status = 'pending';

-- Op 3: Add products
SELECT 'W2-03: Adding new products';
INSERT INTO products VALUES
    (7, 'Monitor', 399.99, 'Electronics', 75),
    (8, 'Keyboard', 79.99, 'Electronics', 150);

-- Delay

-- Op 4: Create table for analytics
SELECT 'W2-04: Creating daily_stats table';
CREATE TABLE daily_stats (
    stat_date DATE,
    total_orders INTEGER,
    total_revenue DECIMAL(12,2)
);

-- Op 5: Update products stock
SELECT 'W2-05: Decreasing Electronics stock';
UPDATE products SET stock = stock - 10 WHERE category = 'Electronics';

-- Op 6: Insert stats
SELECT 'W2-06: Inserting daily stats';
INSERT INTO daily_stats VALUES
    ('2024-01-01', 10, 5050.00),
    ('2024-01-02', 5, 2500.00);

-- Delay

-- Op 7: Add column to orders
SELECT 'W2-07: Adding discount column to orders';
ALTER TABLE orders ADD COLUMN discount DECIMAL(5,2);

-- Op 8: Update orders with discount
SELECT 'W2-08: Setting discounts on orders';
UPDATE orders SET discount = 5.00 WHERE amount > 300;

-- Op 9: Delete low-value orders
SELECT 'W2-09: Deleting orders under $200';
DELETE FROM orders WHERE amount < 200;

-- Delay

-- Op 10: Insert more orders with discount
SELECT 'W2-10: Inserting orders with discount';
INSERT INTO orders (id, customer_id, amount, status, region, created_at, discount) VALUES
    (25, 304, 500.00, 'pending', 'US', '2024-01-03 14:00:00', 10.00);

-- Op 11: Update user emails
SELECT 'W2-11: Updating user email domains';
UPDATE users SET email = REPLACE(email, '@test.com', '@company.com');

-- Op 12: Add products to Furniture
SELECT 'W2-12: Adding Furniture products';
INSERT INTO products VALUES
    (9, 'Bookshelf', 149.99, 'Furniture', 40),
    (10, 'Lamp', 49.99, 'Furniture', 100);

-- Op 13: Delete Office products
SELECT 'W2-13: Deleting Office products';
DELETE FROM products WHERE category = 'Office';

-- Delay

-- Op 14: Update stats
SELECT 'W2-14: Updating daily stats';
UPDATE daily_stats SET total_orders = total_orders + 5 WHERE stat_date = '2024-01-02';

SELECT 'W2: Completed 14 operations';
