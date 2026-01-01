-- Worker: GLOBAL Team - Updates ALL regions by +5%
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_partition_updates host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_partition_updates');
USE t;

CALL ducklake_use_branch('t', 'global_team');
SELECT * FROM ducklake_current_branch('t');

SELECT 'GLOBAL_TEAM: Starting with ' || COUNT(*)::VARCHAR || ' rows' FROM sales;

-- Update ALL regions prices +5% (global adjustment)
UPDATE sales SET amount = amount * 1.05;
SELECT 'GLOBAL_TEAM: Updated ALL prices +5%';

-- Query other branches while working (cross-branch reads)
SELECT 'GLOBAL_TEAM: Main branch total = ' || SUM(amount)::VARCHAR FROM sales AT (BRANCH => 'main');

-- Add global shipping fee to all products
UPDATE sales SET amount = amount + 10.00;
SELECT 'GLOBAL_TEAM: Added $10 shipping to all';

-- Add a LATAM product (new region)
INSERT INTO sales VALUES (104, 'LATAM', 'Laptop', 950.00, 5);
SELECT 'GLOBAL_TEAM: Added LATAM region';

SELECT 'GLOBAL_TEAM: Final state by region:';
SELECT region, COUNT(*) as products, SUM(amount)::VARCHAR as total FROM sales GROUP BY region ORDER BY region;
