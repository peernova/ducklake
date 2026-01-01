-- Worker NYC: Modify NYC warehouse inventory while querying other branches
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_cross host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_cross');
USE t;
CALL ducklake_use_branch('t', 'nyc_ops');

SELECT 'NYC: Starting operations...';

-- Modify NYC inventory
UPDATE inventory SET quantity = quantity + 50 WHERE warehouse = 'NYC';
SELECT 'NYC: Increased NYC stock by 50';

INSERT INTO inventory VALUES (10, 'NYC_Special', 500, 'NYC');
SELECT 'NYC: Added NYC_Special item';

-- Query main branch while working
SELECT 'NYC: Main branch total = ' || SUM(quantity)::VARCHAR FROM inventory AT (BRANCH => 'main');

-- Query own branch
SELECT 'NYC: NYC_ops total = ' || SUM(quantity)::VARCHAR FROM inventory;

-- Query sibling branch (should see original data)
SELECT 'NYC: LA_ops total = ' || SUM(quantity)::VARCHAR FROM inventory AT (BRANCH => 'la_ops');

SELECT 'NYC: Final inventory:';
SELECT * FROM inventory ORDER BY id;
