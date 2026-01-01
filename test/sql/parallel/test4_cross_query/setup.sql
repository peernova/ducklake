-- Test 4: Cross-Branch Queries During Parallel Operations
-- Setup: Create data and branches for cross-query testing
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_cross host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_cross');
USE t;

-- Create inventory table
CREATE TABLE inventory (id INT, item VARCHAR, quantity INT, warehouse VARCHAR);
INSERT INTO inventory VALUES
    (1, 'Widget', 100, 'NYC'),
    (2, 'Gadget', 50, 'LA'),
    (3, 'Gizmo', 75, 'CHI'),
    (4, 'Doodad', 200, 'NYC');
SELECT 'Main: Created inventory with ' || COUNT(*)::VARCHAR || ' items' FROM inventory;

-- Create branches for different warehouse operations
CALL ducklake_create_branch('t', 'nyc_ops');
CALL ducklake_create_branch('t', 'la_ops');
CALL ducklake_create_branch('t', 'chi_ops');
SELECT 'Created 3 warehouse operation branches';
