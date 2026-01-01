-- Worker 4: Mixed heavy operations - schema, data, partitions
-- ~12-15 operations with random delays
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=chaos_stress_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/chaos_stress_test');
USE t;
CALL ducklake_use_branch('t', 'worker_4_branch');

SELECT 'W4: Starting chaos operations...';

-- Delay to mix up timing

-- Op 1: Create metrics table
SELECT 'W4-01: Creating metrics table';
CREATE TABLE metrics (
    id INTEGER,
    metric_name VARCHAR,
    metric_value DECIMAL(12,4),
    recorded_at TIMESTAMP
);

-- Op 2: Insert metrics
SELECT 'W4-02: Inserting metrics';
INSERT INTO metrics VALUES
    (1, 'cpu_usage', 45.5, '2024-01-05 10:00:00'),
    (2, 'memory_usage', 62.3, '2024-01-05 10:00:00'),
    (3, 'disk_usage', 78.9, '2024-01-05 10:00:00');

-- Delay

-- Op 3: Add columns to orders
SELECT 'W4-03: Adding priority column to orders';
ALTER TABLE orders ADD COLUMN priority INTEGER;

-- Op 4: Update orders priority
SELECT 'W4-04: Setting order priorities';
UPDATE orders SET priority = 1 WHERE amount > 500;
UPDATE orders SET priority = 2 WHERE amount <= 500 AND amount > 200;
UPDATE orders SET priority = 3 WHERE amount <= 200;

-- Op 5: Delete cancelled orders
SELECT 'W4-05: Deleting cancelled orders';
DELETE FROM orders WHERE status = 'cancelled';

-- Delay

-- Op 6: Insert orders with priority
SELECT 'W4-06: Inserting high-priority orders';
INSERT INTO orders (id, customer_id, amount, status, region, created_at, priority) VALUES
    (41, 500, 1000.00, 'pending', 'US', '2024-01-05 11:00:00', 1),
    (42, 501, 1500.00, 'pending', 'EU', '2024-01-05 12:00:00', 1);

-- Op 7: Update products - change category (cross-partition)
SELECT 'W4-07: Moving products between categories';
UPDATE products SET category = 'Tech' WHERE category = 'Electronics' AND price > 100;

-- Op 8: Create tags table
SELECT 'W4-08: Creating tags table';
CREATE TABLE tags (
    id INTEGER,
    entity_type VARCHAR,
    entity_id INTEGER,
    tag VARCHAR
);

-- Delay

-- Op 9: Insert tags
SELECT 'W4-09: Inserting tags';
INSERT INTO tags VALUES
    (1, 'product', 1, 'bestseller'),
    (2, 'product', 2, 'sale'),
    (3, 'order', 41, 'priority'),
    (4, 'user', 1, 'vip');

-- Op 10: Delete low-stock products
SELECT 'W4-10: Deleting low-stock products';
DELETE FROM products WHERE stock < 50;

-- Op 11: Add column to users
SELECT 'W4-11: Adding last_login to users';
ALTER TABLE users ADD COLUMN last_login TIMESTAMP;

-- Op 12: Update users last_login
SELECT 'W4-12: Setting last_login';
UPDATE users SET last_login = '2024-01-05 10:00:00' WHERE id <= 2;

-- Delay

-- Op 13: Insert more metrics
SELECT 'W4-13: Inserting more metrics';
INSERT INTO metrics VALUES
    (4, 'network_in', 1024.5, '2024-01-05 10:05:00'),
    (5, 'network_out', 2048.7, '2024-01-05 10:05:00');

-- Op 14: Delete old metrics
SELECT 'W4-14: Deleting metrics';
DELETE FROM metrics WHERE metric_name = 'disk_usage';

-- Op 15: Final update - change order statuses
SELECT 'W4-15: Completing pending orders';
UPDATE orders SET status = 'processing' WHERE status = 'pending';

SELECT 'W4: Completed 15 operations';
