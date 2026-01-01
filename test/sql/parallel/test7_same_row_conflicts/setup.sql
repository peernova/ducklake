-- Setup: Accounts table where multiple branches will modify same rows
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_same_row host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_same_row');
USE t;

-- Create accounts table (no PRIMARY KEY - not supported in DuckLake)
CREATE TABLE accounts (
    id INT,
    name VARCHAR,
    balance DECIMAL(10,2),
    status VARCHAR
);

-- Insert initial data - these exact rows will be modified by multiple branches
INSERT INTO accounts VALUES
    (1, 'Alice', 1000.00, 'active'),
    (2, 'Bob', 2000.00, 'active'),
    (3, 'Charlie', 3000.00, 'active'),
    (4, 'Diana', 4000.00, 'active'),
    (5, 'Eve', 5000.00, 'active');

SELECT 'Main: Created accounts with ' || COUNT(*)::VARCHAR || ' rows' FROM accounts;
SELECT * FROM accounts ORDER BY id;

-- Create 5 branches that will ALL modify the same rows
CALL ducklake_create_branch('t', 'branch_a');
CALL ducklake_create_branch('t', 'branch_b');
CALL ducklake_create_branch('t', 'branch_c');
CALL ducklake_create_branch('t', 'branch_d');
CALL ducklake_create_branch('t', 'branch_e');

SELECT 'Created 5 branches that will modify same rows' as msg;
