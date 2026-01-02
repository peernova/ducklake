-- Worker C: DROP SCHEMA CASCADE (removes entire schema with all objects)
ATTACH 'ducklake:metadata=postgres:host=localhost port=5433 dbname=parallel_drop' AS t (DATA_PATH '/tmp/parallel_drop');
USE t;
CALL ducklake_use_branch('t', 'branch_c');

SELECT 'BRANCH_C: Starting DROP SCHEMA CASCADE test' as msg;

-- Check initial schemas
SELECT 'BRANCH_C: Initial schemas:' as msg;
SELECT schema_name FROM information_schema.schemata
WHERE catalog_name = 't' AND schema_name NOT IN ('information_schema', 'pg_catalog')
ORDER BY schema_name;

-- Query reporting schema before drop
SELECT 'BRANCH_C: reporting.daily_stats before drop:' as msg;
SELECT * FROM reporting.daily_stats;

SELECT 'BRANCH_C: reporting.summary view before drop:' as msg;
SELECT * FROM reporting.summary;

-- Drop entire schema
DROP SCHEMA reporting CASCADE;
SELECT 'BRANCH_C: Dropped reporting schema CASCADE' as msg;

-- Verify schema is gone
SELECT 'BRANCH_C: Schemas after DROP:' as msg;
SELECT schema_name FROM information_schema.schemata
WHERE catalog_name = 't' AND schema_name NOT IN ('information_schema', 'pg_catalog')
ORDER BY schema_name;

-- Other schemas still work
SELECT 'BRANCH_C: sales.orders still works:' as msg;
SELECT COUNT(*) as count FROM sales.orders;

SELECT 'BRANCH_C: analytics views still work:' as msg;
SELECT * FROM analytics.order_summary ORDER BY customer;

-- Create a new schema
CREATE SCHEMA branch_c_reports;
CREATE TABLE branch_c_reports.custom_stats (id INT, value INT);
INSERT INTO branch_c_reports.custom_stats VALUES (1, 100), (2, 200);

SELECT 'BRANCH_C: Created new schema branch_c_reports' as msg;
SELECT * FROM branch_c_reports.custom_stats;
