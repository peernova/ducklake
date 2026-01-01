-- Setup: Create initial table with basic schema
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=schema_evolution_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/schema_evolution_test');
USE t;

-- Create initial table
CREATE TABLE users (
    id INTEGER,
    name VARCHAR
);

-- Insert initial data
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');

SELECT 'Initial setup: users table with 3 rows (id, name)';
SELECT * FROM users ORDER BY id;
