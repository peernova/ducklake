-- Worker V2: Add shipping and tracking columns
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_schema host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_schema');
USE t;
CALL ducklake_use_branch('t', 'schema_v2');

SELECT 'V2: Starting schema evolution...';

ALTER TABLE orders ADD COLUMN shipping_address VARCHAR;
SELECT 'V2: Added shipping_address column';

UPDATE orders SET shipping_address = customer || ' Street 123';
SELECT 'V2: Set shipping addresses';

ALTER TABLE orders ADD COLUMN tracking_number VARCHAR;
SELECT 'V2: Added tracking_number column';

INSERT INTO orders (id, customer, amount, shipping_address, tracking_number) VALUES (20, 'V2_Customer', 300.00, '456 Oak Ave', 'TRK-20-001');
SELECT 'V2: Inserted new order with shipping info';

SELECT 'V2: Final schema and data:';
SELECT * FROM orders ORDER BY id;
