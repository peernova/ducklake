-- Worker A: MERGE on branch_a - UPDATE existing + INSERT new
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=parallel_merge host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_merge');
USE t;
CALL ducklake_use_branch('t', 'branch_a');

SELECT 'Worker A: Starting MERGE on branch_a' as msg;

-- Create source table with updates and inserts
CREATE TEMP TABLE source_a AS
SELECT * FROM (VALUES
    (1, 'Widget A Updated', 15.00, 150),   -- UPDATE existing
    (2, 'Widget B Updated', 25.00, 250),   -- UPDATE existing
    (6, 'Widget F', 60.00, 600),           -- INSERT new
    (7, 'Widget G', 70.00, 700)            -- INSERT new
) AS t(product_id, name, price, stock);

-- Execute MERGE
MERGE INTO products p
USING source_a s ON p.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET name = s.name, price = s.price, stock = s.stock
WHEN NOT MATCHED THEN INSERT VALUES (s.product_id, s.name, s.price, s.stock);

SELECT 'Worker A: MERGE complete, products:' as msg;
SELECT * FROM products ORDER BY product_id;

SELECT 'Worker A: Done' as msg;
