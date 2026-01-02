-- Worker D: Multiple DROP operations (table + view + schema)
ATTACH 'ducklake:metadata=postgres:host=localhost port=5433 dbname=parallel_drop' AS t (DATA_PATH '/tmp/parallel_drop');
USE t;
CALL ducklake_use_branch('t', 'branch_d');

SELECT 'BRANCH_D: Starting multiple DROP operations test' as msg;

-- Check initial state
SELECT 'BRANCH_D: Initial schemas:' as msg;
SELECT schema_name FROM information_schema.schemata
WHERE catalog_name = 't' AND schema_name NOT IN ('information_schema', 'pg_catalog')
ORDER BY schema_name;

-- Drop a table from sales
DROP TABLE sales.customers;
SELECT 'BRANCH_D: Dropped sales.customers' as msg;

-- Drop a view from analytics
DROP VIEW analytics.top_customers;
SELECT 'BRANCH_D: Dropped analytics.top_customers' as msg;

-- Drop entire inventory schema
DROP SCHEMA inventory CASCADE;
SELECT 'BRANCH_D: Dropped inventory schema CASCADE' as msg;

-- Verify state
SELECT 'BRANCH_D: Remaining schemas:' as msg;
SELECT schema_name FROM information_schema.schemata
WHERE catalog_name = 't' AND schema_name NOT IN ('information_schema', 'pg_catalog')
ORDER BY schema_name;

SELECT 'BRANCH_D: Remaining tables in sales:' as msg;
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'sales' AND table_catalog = 't'
ORDER BY table_name;

SELECT 'BRANCH_D: Remaining views in analytics:' as msg;
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'analytics' AND table_type = 'VIEW' AND table_catalog = 't'
ORDER BY table_name;

-- Can still use remaining objects
SELECT 'BRANCH_D: Can still query sales.orders:' as msg;
SELECT * FROM sales.orders ORDER BY id;

SELECT 'BRANCH_D: Can still query analytics.order_summary:' as msg;
SELECT * FROM analytics.order_summary ORDER BY customer;

-- Add data to prove branch is working
INSERT INTO sales.orders VALUES (200, 'BranchD_Customer', 500.00);
SELECT 'BRANCH_D: Added new order' as msg;
