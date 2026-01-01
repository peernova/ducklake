-- Worker: Operations on feature_a_hotfix branch (grandchild level)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_nested host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_nested');
USE t;
CALL ducklake_use_branch('t', 'feature_a_hotfix');

SELECT 'HOTFIX: Starting with ' || COUNT(*)::VARCHAR || ' products' FROM products;

-- Hotfix: urgent price correction
UPDATE products SET price = 249.99 WHERE id = 6;
SELECT 'HOTFIX: Fixed Monitor price to 249.99';

-- Emergency product addition
INSERT INTO products VALUES (999, 'EmergencyPatch', 0.00, 'hotfix');
SELECT 'HOTFIX: Added EmergencyPatch (id=999)';

SELECT 'HOTFIX: Final count = ' || COUNT(*)::VARCHAR FROM products;
SELECT * FROM products ORDER BY id;
