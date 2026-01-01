-- Worker: Heavy INSERTs - 50 new rows
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_schema_dml host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_schema_dml');
USE t;

CALL ducklake_use_branch('t', 'dml_heavy');
SELECT * FROM ducklake_current_branch('t');

SELECT 'DML_HEAVY: Starting heavy inserts...';
SELECT 'DML_HEAVY: Initial count = ' || COUNT(*)::VARCHAR FROM products;

-- Insert 50 new products
INSERT INTO products
SELECT
    1000 + i as id,
    'Product_' || i::VARCHAR as name,
    (10 + (i % 100))::DECIMAL(10,2) as price,
    CASE i % 4
        WHEN 0 THEN 'electronics'
        WHEN 1 THEN 'furniture'
        WHEN 2 THEN 'accessories'
        ELSE 'other'
    END as category
FROM generate_series(1, 50) as t(i);

SELECT 'DML_HEAVY: Inserted 50 rows';
SELECT 'DML_HEAVY: Final count = ' || COUNT(*)::VARCHAR FROM products;

SELECT 'DML_HEAVY: Category distribution:';
SELECT category, COUNT(*) as cnt FROM products GROUP BY category ORDER BY category;
