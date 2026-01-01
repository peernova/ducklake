-- Worker CHI: Modify CHI warehouse inventory while querying other branches
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_cross host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_cross');
USE t;
CALL ducklake_use_branch('t', 'chi_ops');

SELECT 'CHI: Starting operations...';

-- Modify CHI inventory
UPDATE inventory SET quantity = 0 WHERE warehouse = 'CHI';
SELECT 'CHI: Cleared CHI stock (quantity=0)';

INSERT INTO inventory VALUES (30, 'CHI_Bulk', 5000, 'CHI');
SELECT 'CHI: Added bulk inventory';

-- Query main branch while working
SELECT 'CHI: Main branch total = ' || SUM(quantity)::VARCHAR FROM inventory AT (BRANCH => 'main');

-- Query own branch
SELECT 'CHI: CHI_ops total = ' || SUM(quantity)::VARCHAR FROM inventory;

-- Query sibling branches (should see original data)
SELECT 'CHI: NYC_ops total = ' || SUM(quantity)::VARCHAR FROM inventory AT (BRANCH => 'nyc_ops');
SELECT 'CHI: LA_ops total = ' || SUM(quantity)::VARCHAR FROM inventory AT (BRANCH => 'la_ops');

SELECT 'CHI: Final inventory:';
SELECT * FROM inventory ORDER BY id;
