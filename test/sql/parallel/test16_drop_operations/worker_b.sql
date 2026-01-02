-- Worker B: DROP VIEW operations in analytics schema
ATTACH 'ducklake:metadata=postgres:host=localhost port=5433 dbname=parallel_drop' AS t (DATA_PATH '/tmp/parallel_drop');
USE t;
CALL ducklake_use_branch('t', 'branch_b');

SELECT 'BRANCH_B: Starting DROP VIEW test' as msg;

-- Check initial views
SELECT 'BRANCH_B: Initial views:' as msg;
SELECT table_name FROM information_schema.tables
WHERE table_type = 'VIEW' AND table_catalog = 't'
ORDER BY table_name;

-- Query views before dropping
SELECT 'BRANCH_B: order_summary before drop:' as msg;
SELECT * FROM analytics.order_summary ORDER BY customer;

-- Drop one view
DROP VIEW analytics.order_summary;
SELECT 'BRANCH_B: Dropped analytics.order_summary' as msg;

-- Verify it's gone
SELECT 'BRANCH_B: Views after DROP:' as msg;
SELECT table_name FROM information_schema.tables
WHERE table_type = 'VIEW' AND table_catalog = 't'
ORDER BY table_name;

-- Other views still work
SELECT 'BRANCH_B: top_customers still works:' as msg;
SELECT * FROM analytics.top_customers ORDER BY customer;

-- Create a new view to replace it
CREATE VIEW analytics.order_summary_v2 AS
SELECT customer, SUM(amount) as total, COUNT(*) as num_orders
FROM sales.orders GROUP BY customer;

SELECT 'BRANCH_B: Created replacement view order_summary_v2' as msg;
SELECT * FROM analytics.order_summary_v2 ORDER BY customer;
