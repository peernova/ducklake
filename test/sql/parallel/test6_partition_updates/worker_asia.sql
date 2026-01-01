-- Worker: ASIA Team - Updates ASIA region prices by +15%
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_partition_updates host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_partition_updates');
USE t;

CALL ducklake_use_branch('t', 'asia_team');
SELECT * FROM ducklake_current_branch('t');

SELECT 'ASIA_TEAM: Starting with ' || COUNT(*)::VARCHAR || ' rows' FROM sales;

-- Update ASIA region prices +15%
UPDATE sales SET amount = amount * 1.15 WHERE region = 'ASIA';
SELECT 'ASIA_TEAM: Updated ASIA prices +15%';

-- Double ASIA quantities (high demand)
UPDATE sales SET quantity = quantity * 2 WHERE region = 'ASIA';
SELECT 'ASIA_TEAM: Doubled ASIA quantities';

-- Add two new ASIA products
INSERT INTO sales VALUES
    (102, 'ASIA', 'Smartwatch', 200.00, 50),
    (103, 'ASIA', 'Earbuds', 100.00, 100);
SELECT 'ASIA_TEAM: Added Smartwatch and Earbuds';

SELECT 'ASIA_TEAM: Final state:';
SELECT region, product, amount, quantity FROM sales WHERE region = 'ASIA' ORDER BY id;
SELECT 'ASIA_TEAM: Total ASIA amount = ' || SUM(amount)::VARCHAR FROM sales WHERE region = 'ASIA';
