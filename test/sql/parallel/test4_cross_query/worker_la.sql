-- Worker LA: Modify LA warehouse inventory while querying other branches
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_cross host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_cross');
USE t;
CALL ducklake_use_branch('t', 'la_ops');

SELECT 'LA: Starting operations...';

-- Modify LA inventory
DELETE FROM inventory WHERE id = 2;
SELECT 'LA: Removed Gadget (id=2)';

INSERT INTO inventory VALUES (20, 'LA_Exclusive', 1000, 'LA');
INSERT INTO inventory VALUES (21, 'LA_Premium', 300, 'LA');
SELECT 'LA: Added 2 LA exclusive items';

-- Query main branch while working
SELECT 'LA: Main branch total = ' || SUM(quantity)::VARCHAR FROM inventory AT (BRANCH => 'main');

-- Query own branch
SELECT 'LA: LA_ops total = ' || SUM(quantity)::VARCHAR FROM inventory;

-- Query sibling branch (should see original data)
SELECT 'LA: CHI_ops total = ' || SUM(quantity)::VARCHAR FROM inventory AT (BRANCH => 'chi_ops');

SELECT 'LA: Final inventory:';
SELECT * FROM inventory ORDER BY id;
