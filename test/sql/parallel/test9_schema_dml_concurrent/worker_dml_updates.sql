-- Worker: Multiple UPDATE passes on all rows
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_schema_dml host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_schema_dml');
USE t;

CALL ducklake_use_branch('t', 'dml_updates');
SELECT * FROM ducklake_current_branch('t');

SELECT 'DML_UPDATES: Starting update passes...';
SELECT 'DML_UPDATES: Initial total price = ' || SUM(price)::VARCHAR FROM products;

-- Pass 1: Increase all prices by 10%
UPDATE products SET price = price * 1.10;
SELECT 'DML_UPDATES: Pass 1 - prices +10%';

-- Pass 2: Add category prefix to names
UPDATE products SET name = category || '_' || name;
SELECT 'DML_UPDATES: Pass 2 - prefixed names';

-- Pass 3: Another price increase
UPDATE products SET price = price * 1.05;
SELECT 'DML_UPDATES: Pass 3 - prices +5%';

-- Pass 4: Update specific categories
UPDATE products SET price = price + 50.00 WHERE category = 'electronics';
SELECT 'DML_UPDATES: Pass 4 - electronics +$50';

SELECT 'DML_UPDATES: Final total price = ' || SUM(price)::VARCHAR FROM products;
SELECT 'DML_UPDATES: Sample data:';
SELECT * FROM products ORDER BY id LIMIT 5;
