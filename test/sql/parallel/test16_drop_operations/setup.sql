-- Setup: Create schemas, tables, and views on main branch
ATTACH 'ducklake:metadata=postgres:host=localhost port=5433 dbname=parallel_drop' AS t (DATA_PATH '/tmp/parallel_drop');
USE t;

-- Create sales schema with tables
CREATE SCHEMA sales;
CREATE TABLE sales.orders (id INT, customer VARCHAR, amount DECIMAL(10,2));
INSERT INTO sales.orders VALUES (1, 'Alice', 100.00), (2, 'Bob', 200.00), (3, 'Charlie', 150.00);

CREATE TABLE sales.customers (id INT, name VARCHAR, email VARCHAR);
INSERT INTO sales.customers VALUES (1, 'Alice', 'alice@test.com'), (2, 'Bob', 'bob@test.com');

CREATE TABLE sales.products (id INT, name VARCHAR, price DECIMAL(10,2));
INSERT INTO sales.products VALUES (1, 'Widget', 25.00), (2, 'Gadget', 50.00);

-- Create analytics schema with views
CREATE SCHEMA analytics;
CREATE TABLE analytics.metrics (id INT, metric_name VARCHAR, value DECIMAL(10,2));
INSERT INTO analytics.metrics VALUES (1, 'revenue', 1500.00), (2, 'orders', 10.00);

CREATE VIEW analytics.order_summary AS
SELECT customer, SUM(amount) as total FROM sales.orders GROUP BY customer;

CREATE VIEW analytics.top_customers AS
SELECT customer, SUM(amount) as total FROM sales.orders GROUP BY customer HAVING SUM(amount) > 100;

-- Create reporting schema
CREATE SCHEMA reporting;
CREATE TABLE reporting.daily_stats (report_date DATE, total_orders INT, total_revenue DECIMAL(10,2));
INSERT INTO reporting.daily_stats VALUES ('2024-01-01', 5, 500.00), ('2024-01-02', 8, 800.00);

CREATE VIEW reporting.summary AS SELECT SUM(total_orders) as orders, SUM(total_revenue) as revenue FROM reporting.daily_stats;

-- Create inventory schema
CREATE SCHEMA inventory;
CREATE TABLE inventory.stock (id INT, product_id INT, quantity INT);
INSERT INTO inventory.stock VALUES (1, 1, 100), (2, 2, 50);

CREATE VIEW inventory.low_stock AS SELECT * FROM inventory.stock WHERE quantity < 60;

-- Show initial state
SELECT 'Setup complete - schemas:' as msg;
SELECT schema_name FROM information_schema.schemata
WHERE catalog_name = 't' AND schema_name NOT IN ('information_schema', 'pg_catalog')
ORDER BY schema_name;

-- Create 4 branches for parallel testing
CALL ducklake_create_branch('t', 'branch_a');
CALL ducklake_create_branch('t', 'branch_b');
CALL ducklake_create_branch('t', 'branch_c');
CALL ducklake_create_branch('t', 'branch_d');

SELECT 'Created 4 branches for parallel DROP testing' as msg;
