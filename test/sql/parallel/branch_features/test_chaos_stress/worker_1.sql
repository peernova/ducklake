-- Worker 1: Heavy on schema changes and cross-partition operations
-- ~12-15 operations with random delays
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=chaos_stress_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/chaos_stress_test');
USE t;
CALL ducklake_use_branch('t', 'worker_1_branch');

SELECT 'W1: Starting chaos operations...';

-- Op 1: Add column to users
SELECT 'W1-01: Adding phone column to users';
ALTER TABLE users ADD COLUMN phone VARCHAR;

-- Op 2: Insert into orders (different partition values)
SELECT 'W1-02: Inserting mixed status orders';
INSERT INTO orders VALUES
    (11, 200, 100.00, 'pending', 'US', '2024-01-02 10:00:00'),
    (12, 201, 200.00, 'completed', 'EU', '2024-01-02 11:00:00');

-- Delay

-- Op 3: Update orders crossing partition boundary (status change)
SELECT 'W1-03: Changing order status (cross-partition update)';
UPDATE orders SET status = 'completed' WHERE id = 1;

-- Op 4: Create new table
SELECT 'W1-04: Creating new table audit_log';
CREATE TABLE audit_log (
    id INTEGER,
    action VARCHAR,
    table_name VARCHAR,
    timestamp TIMESTAMP
);

-- Op 5: Insert into new table
SELECT 'W1-05: Inserting into audit_log';
INSERT INTO audit_log VALUES (1, 'INSERT', 'orders', '2024-01-02 12:00:00');

-- Delay

-- Op 6: Delete from products (non-partition column filter)
SELECT 'W1-06: Deleting products with stock < 100';
DELETE FROM products WHERE stock < 100;

-- Op 7: Add another column to users
SELECT 'W1-07: Adding address column to users';
ALTER TABLE users ADD COLUMN address VARCHAR;

-- Op 8: Update products price (partition column untouched)
SELECT 'W1-08: Updating Electronics prices +10%';
UPDATE products SET price = price * 1.1 WHERE category = 'Electronics';

-- Delay

-- Op 9: Insert more users with new columns
SELECT 'W1-09: Inserting users with new columns';
INSERT INTO users VALUES (5, 'Eve', 'eve@test.com', 'user', '555-0001', '123 Main St');

-- Op 10: Delete from orders by region (non-partition column)
SELECT 'W1-10: Deleting ASIA orders';
DELETE FROM orders WHERE region = 'ASIA';

-- Op 11: Create another new table
SELECT 'W1-11: Creating inventory table';
CREATE TABLE inventory (
    product_id INTEGER,
    warehouse VARCHAR,
    quantity INTEGER
);

-- Delay

-- Op 12: Insert into inventory
SELECT 'W1-12: Populating inventory';
INSERT INTO inventory VALUES (1, 'NYC', 100), (2, 'LA', 200), (3, 'CHI', 150);

-- Op 13: Update users
SELECT 'W1-13: Updating user roles';
UPDATE users SET role = 'senior_user' WHERE role = 'user';

-- Op 14: Delete cancelled orders (partition delete)
SELECT 'W1-14: Deleting cancelled orders';
DELETE FROM orders WHERE status = 'cancelled';

SELECT 'W1: Completed 14 operations';
