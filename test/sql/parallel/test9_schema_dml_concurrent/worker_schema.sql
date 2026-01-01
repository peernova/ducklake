-- Worker: Schema changes - ADD COLUMN, DROP COLUMN, repartition
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_schema_dml host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_schema_dml');
USE t;

CALL ducklake_use_branch('t', 'schema_worker');
SELECT * FROM ducklake_current_branch('t');

SELECT 'SCHEMA: Starting schema modifications...';

-- Add discount column
ALTER TABLE products ADD COLUMN discount DECIMAL(5,2) DEFAULT 0.00;
SELECT 'SCHEMA: Added discount column';

-- Add stock column
ALTER TABLE products ADD COLUMN stock INT DEFAULT 100;
SELECT 'SCHEMA: Added stock column';

-- Update the new columns
UPDATE products SET discount = 10.00 WHERE category = 'electronics';
UPDATE products SET stock = 50 WHERE category = 'accessories';
SELECT 'SCHEMA: Updated new columns';

-- Partition by category
ALTER TABLE products SET PARTITIONED BY (category);
SELECT 'SCHEMA: Partitioned by category';

-- Insert data with new columns
INSERT INTO products VALUES (101, 'GPU', 599.99, 'electronics', 15.00, 25);
SELECT 'SCHEMA: Inserted row with new columns';

SELECT 'SCHEMA: Final schema:';
SELECT column_name FROM (DESCRIBE products);

SELECT 'SCHEMA: Final data sample:';
SELECT * FROM products WHERE id IN (1, 101) ORDER BY id;
