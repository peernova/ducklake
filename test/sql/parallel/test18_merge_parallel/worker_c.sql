-- Worker C: MERGE on branch_c - DELETE via MERGE + INSERT
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=parallel_merge host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_merge');
USE t;
CALL ducklake_use_branch('t', 'branch_c');

SELECT 'Worker C: Starting MERGE on branch_c' as msg;

-- Create source table for MERGE - DELETE product 5, INSERT new products 10, 11
CREATE TEMP TABLE source_c AS
SELECT * FROM (VALUES
    (5, 'DELETE', 0.00, 0),                -- DELETE existing (any matched row gets deleted)
    (10, 'Widget J', 100.00, 1000),        -- INSERT new
    (11, 'Widget K', 110.00, 1100)         -- INSERT new
) AS t(product_id, name, price, stock);

-- Execute MERGE with DELETE + INSERT (supported by DuckLake)
MERGE INTO products p
USING source_c s ON p.product_id = s.product_id
WHEN MATCHED THEN DELETE
WHEN NOT MATCHED THEN INSERT VALUES (s.product_id, s.name, s.price, s.stock);

SELECT 'Worker C: MERGE complete, products:' as msg;
SELECT * FROM products ORDER BY product_id;

SELECT 'Worker C: Done' as msg;
