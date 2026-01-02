-- Worker A: DROP TABLE operations in sales schema
ATTACH 'ducklake:metadata=postgres:host=localhost port=5433 dbname=parallel_drop' AS t (DATA_PATH '/tmp/parallel_drop');
USE t;
CALL ducklake_use_branch('t', 'branch_a');

SELECT 'BRANCH_A: Starting DROP TABLE test' as msg;

-- Check initial state
SELECT 'BRANCH_A: Initial tables in sales schema:' as msg;
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'sales' AND table_catalog = 't'
ORDER BY table_name;

-- Drop one table
DROP TABLE sales.products;
SELECT 'BRANCH_A: Dropped sales.products' as msg;

-- Verify it's gone on this branch
SELECT 'BRANCH_A: Tables after DROP:' as msg;
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'sales' AND table_catalog = 't'
ORDER BY table_name;

-- But can still access other tables
SELECT 'BRANCH_A: Can still query sales.orders:' as msg;
SELECT COUNT(*) as order_count FROM sales.orders;

-- Add new data to remaining tables
INSERT INTO sales.orders VALUES (100, 'BranchA_Customer', 999.99);
SELECT 'BRANCH_A: Added new order' as msg;

SELECT 'BRANCH_A: Final state' as msg;
SELECT * FROM sales.orders ORDER BY id;
