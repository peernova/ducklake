-- Worker V1: Add status column and discount column
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_schema host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_schema');
USE t;
CALL ducklake_use_branch('t', 'schema_v1');

SELECT 'V1: Starting schema evolution...';

ALTER TABLE orders ADD COLUMN status VARCHAR DEFAULT 'pending';
SELECT 'V1: Added status column';

UPDATE orders SET status = 'completed' WHERE amount > 100;
SELECT 'V1: Updated status for high-value orders';

ALTER TABLE orders ADD COLUMN discount DECIMAL(5,2) DEFAULT 0.00;
SELECT 'V1: Added discount column';

INSERT INTO orders (id, customer, amount, status, discount) VALUES (10, 'V1_Customer', 500.00, 'vip', 10.00);
SELECT 'V1: Inserted new order with all columns';

SELECT 'V1: Final schema and data:';
SELECT * FROM orders ORDER BY id;
