-- Worker 2: Add 'phone' column
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=schema_evolution_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/schema_evolution_test');
USE t;

SELECT 'W2: Adding phone column...';
ALTER TABLE users ADD COLUMN phone VARCHAR;
SELECT 'W2: Done adding phone column';
