-- Test 2: Nested Branches (Parent -> Child -> Grandchild) with Parallel Operations
-- Setup: Create base data and nested branch hierarchy
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_nested host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_nested');
USE t;

-- Create products table
CREATE TABLE products (id INT, name VARCHAR, price DECIMAL(10,2), category VARCHAR);
INSERT INTO products VALUES
    (1, 'Laptop', 999.99, 'electronics'),
    (2, 'Mouse', 29.99, 'electronics'),
    (3, 'Desk', 199.99, 'furniture'),
    (4, 'Chair', 149.99, 'furniture');
SELECT 'Main: ' || COUNT(*)::VARCHAR || ' products' FROM products;

-- Create parent branch
CALL ducklake_create_branch('t', 'dev');
SELECT 'Created branch: dev';

-- Switch to dev and create child branches
CALL ducklake_use_branch('t', 'dev');
INSERT INTO products VALUES (5, 'Keyboard', 79.99, 'electronics');
SELECT 'Dev: Added keyboard';

-- Create child branches from dev
CALL ducklake_create_branch('t', 'feature_a');
CALL ducklake_create_branch('t', 'feature_b');
SELECT 'Created child branches: feature_a, feature_b (from dev)';

-- Switch to feature_a and create grandchild
CALL ducklake_use_branch('t', 'feature_a');
INSERT INTO products VALUES (6, 'Monitor', 299.99, 'electronics');
CALL ducklake_create_branch('t', 'feature_a_hotfix');
SELECT 'Created grandchild: feature_a_hotfix (from feature_a)';

SELECT 'Setup complete - branch hierarchy:';
SELECT '  main';
SELECT '    └── dev';
SELECT '          ├── feature_a';
SELECT '          │     └── feature_a_hotfix';
SELECT '          └── feature_b';
