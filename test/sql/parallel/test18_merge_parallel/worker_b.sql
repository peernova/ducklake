-- Worker B: MERGE on branch_b - different updates + inserts
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=parallel_merge host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_merge');
USE t;
CALL ducklake_use_branch('t', 'branch_b');

SELECT 'Worker B: Starting MERGE on branch_b' as msg;

-- Create source table with different updates and inserts
CREATE TEMP TABLE source_b AS
SELECT * FROM (VALUES
    (3, 'Widget C Modified', 35.00, 350),  -- UPDATE existing (different from A)
    (4, 'Widget D Modified', 45.00, 450),  -- UPDATE existing (different from A)
    (8, 'Widget H', 80.00, 800),           -- INSERT new (different from A)
    (9, 'Widget I', 90.00, 900)            -- INSERT new (different from A)
) AS t(product_id, name, price, stock);

-- Execute MERGE
MERGE INTO products p
USING source_b s ON p.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET name = s.name, price = s.price, stock = s.stock
WHEN NOT MATCHED THEN INSERT VALUES (s.product_id, s.name, s.price, s.stock);

SELECT 'Worker B: MERGE complete, products:' as msg;
SELECT * FROM products ORDER BY product_id;

SELECT 'Worker B: Done' as msg;
