-- Worker 3: Heavy on deletes and partition changes
-- ~12-15 operations with random delays
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=chaos_stress_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/chaos_stress_test');
USE t;
CALL ducklake_use_branch('t', 'worker_3_branch');

SELECT 'W3: Starting chaos operations...';

-- Delay to mix up timing

-- Op 1: Delete completed orders
SELECT 'W3-01: Deleting completed orders';
DELETE FROM orders WHERE status = 'completed';

-- Op 2: Change partition key on products
SELECT 'W3-02: Changing products partition to price ranges';
ALTER TABLE products SET PARTITIONED BY (name);

-- Delay

-- Op 3: Insert orders
SELECT 'W3-03: Inserting new orders';
INSERT INTO orders VALUES
    (31, 400, 125.00, 'pending', 'US', '2024-01-04 10:00:00'),
    (32, 401, 225.00, 'completed', 'EU', '2024-01-04 11:00:00');

-- Op 4: Create events table
SELECT 'W3-04: Creating events table';
CREATE TABLE events (
    id INTEGER,
    event_type VARCHAR,
    payload VARCHAR,
    created_at TIMESTAMP
);

-- Op 5: Insert events
SELECT 'W3-05: Inserting events';
INSERT INTO events VALUES
    (1, 'order_created', '{"order_id": 31}', '2024-01-04 10:00:00'),
    (2, 'order_created', '{"order_id": 32}', '2024-01-04 11:00:00');

-- Delay

-- Op 6: Delete users by role
SELECT 'W3-06: Deleting admin users';
DELETE FROM users WHERE role = 'admin';

-- Op 7: Add column to products
SELECT 'W3-07: Adding supplier column to products';
ALTER TABLE products ADD COLUMN supplier VARCHAR;

-- Op 8: Update products with supplier
SELECT 'W3-08: Setting suppliers';
UPDATE products SET supplier = 'SupplierA' WHERE id <= 3;
UPDATE products SET supplier = 'SupplierB' WHERE id > 3;

-- Delay

-- Op 9: Delete pending orders
SELECT 'W3-09: Deleting pending orders';
DELETE FROM orders WHERE status = 'pending';

-- Op 10: Insert more products
SELECT 'W3-10: Adding more products';
INSERT INTO products VALUES
    (11, 'Webcam', 89.99, 'Electronics', 60, 'SupplierC'),
    (12, 'Headphones', 149.99, 'Electronics', 80, 'SupplierC');

-- Op 11: Create sessions table
SELECT 'W3-11: Creating sessions table';
CREATE TABLE sessions (
    session_id VARCHAR,
    user_id INTEGER,
    started_at TIMESTAMP,
    ended_at TIMESTAMP
);

-- Delay

-- Op 12: Insert sessions
SELECT 'W3-12: Inserting sessions';
INSERT INTO sessions VALUES
    ('sess_001', 1, '2024-01-04 09:00:00', '2024-01-04 10:00:00'),
    ('sess_002', 2, '2024-01-04 09:30:00', NULL);

-- Op 13: Update orders region
SELECT 'W3-13: Changing order regions';
UPDATE orders SET region = 'GLOBAL' WHERE region = 'EU';

-- Op 14: Delete events
SELECT 'W3-14: Deleting old events';
DELETE FROM events WHERE created_at < '2024-01-04 11:00:00';

SELECT 'W3: Completed 14 operations';
