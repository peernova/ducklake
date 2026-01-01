-- Worker: Operations on feature_a branch (child level)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_nested host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_nested');
USE t;
CALL ducklake_use_branch('t', 'feature_a');

SELECT 'FEATURE_A: Starting with ' || COUNT(*)::VARCHAR || ' products' FROM products;

-- Delete a furniture item
DELETE FROM products WHERE id = 3;
SELECT 'FEATURE_A: Deleted Desk (id=3)';

-- Add feature_a specific products
INSERT INTO products VALUES (200, 'FeatureA_Widget', 19.99, 'widgets');
INSERT INTO products VALUES (201, 'FeatureA_Gadget', 39.99, 'gadgets');
SELECT 'FEATURE_A: Added 2 new products (200, 201)';

SELECT 'FEATURE_A: Final count = ' || COUNT(*)::VARCHAR FROM products;
SELECT * FROM products ORDER BY id;
