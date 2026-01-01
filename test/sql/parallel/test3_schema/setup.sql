-- Test 3: Concurrent Schema Changes on Different Branches
-- Setup: Create base table and branches for schema evolution testing
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_schema host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_schema');
USE t;

-- Create orders table with minimal schema
CREATE TABLE orders (id INT, customer VARCHAR, amount DECIMAL(10,2));
INSERT INTO orders VALUES
    (1, 'Alice', 100.00),
    (2, 'Bob', 250.00),
    (3, 'Charlie', 75.50);
SELECT 'Main: Created orders with ' || COUNT(*)::VARCHAR || ' rows' FROM orders;

-- Create 3 branches that will each evolve schema differently
CALL ducklake_create_branch('t', 'schema_v1');
CALL ducklake_create_branch('t', 'schema_v2');
CALL ducklake_create_branch('t', 'schema_v3');
SELECT 'Created 3 branches for parallel schema evolution';
