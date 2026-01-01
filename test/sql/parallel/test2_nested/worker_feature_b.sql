-- Worker: Operations on feature_b branch (child level, sibling to feature_a)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_nested host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_nested');
USE t;
CALL ducklake_use_branch('t', 'feature_b');

SELECT 'FEATURE_B: Starting with ' || COUNT(*)::VARCHAR || ' products' FROM products;

-- Different operations than sibling branch
UPDATE products SET category = 'premium-' || category WHERE price > 100;
SELECT 'FEATURE_B: Updated category prefix for expensive items';

INSERT INTO products VALUES (300, 'FeatureB_Tool', 89.99, 'tools');
SELECT 'FEATURE_B: Added FeatureB_Tool (id=300)';

SELECT 'FEATURE_B: Final count = ' || COUNT(*)::VARCHAR FROM products;
SELECT * FROM products ORDER BY id;
