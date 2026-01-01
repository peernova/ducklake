-- Worker: Operations on dev branch (parent level)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_nested host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_nested');
USE t;
CALL ducklake_use_branch('t', 'dev');

SELECT 'DEV: Starting with ' || COUNT(*)::VARCHAR || ' products' FROM products;

-- Update prices in dev
UPDATE products SET price = price * 1.1 WHERE category = 'electronics';
SELECT 'DEV: Increased electronics prices by 10%';

-- Add new product
INSERT INTO products VALUES (100, 'DevProduct', 49.99, 'dev-only');
SELECT 'DEV: Added DevProduct (id=100)';

SELECT 'DEV: Final count = ' || COUNT(*)::VARCHAR FROM products;
SELECT * FROM products ORDER BY id;
