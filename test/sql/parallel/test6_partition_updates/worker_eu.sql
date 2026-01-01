-- Worker: EU Team - Updates EU region prices by +20%
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_partition_updates host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_partition_updates');
USE t;

CALL ducklake_use_branch('t', 'eu_team');
SELECT * FROM ducklake_current_branch('t');

SELECT 'EU_TEAM: Starting with ' || COUNT(*)::VARCHAR || ' rows' FROM sales;

-- Update EU region prices +20%
UPDATE sales SET amount = amount * 1.20 WHERE region = 'EU';
SELECT 'EU_TEAM: Updated EU prices +20%';

-- Decrease EU quantities (clearance sale)
UPDATE sales SET quantity = quantity - 3 WHERE region = 'EU';
SELECT 'EU_TEAM: Decreased EU quantities -3';

-- Delete one EU product
DELETE FROM sales WHERE region = 'EU' AND product = 'Tablet';
SELECT 'EU_TEAM: Removed Tablet from EU';

SELECT 'EU_TEAM: Final state:';
SELECT region, product, amount, quantity FROM sales WHERE region = 'EU' ORDER BY id;
SELECT 'EU_TEAM: Total EU amount = ' || SUM(amount)::VARCHAR FROM sales WHERE region = 'EU';
