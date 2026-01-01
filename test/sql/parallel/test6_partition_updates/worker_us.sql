-- Worker: US Team - Updates US region prices by +10%
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_partition_updates host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_partition_updates');
USE t;

CALL ducklake_use_branch('t', 'us_team');
SELECT * FROM ducklake_current_branch('t');

SELECT 'US_TEAM: Starting with ' || COUNT(*)::VARCHAR || ' rows' FROM sales;

-- Update US region prices +10%
UPDATE sales SET amount = amount * 1.10 WHERE region = 'US';
SELECT 'US_TEAM: Updated US prices +10%';

-- Also increase US quantities
UPDATE sales SET quantity = quantity + 5 WHERE region = 'US';
SELECT 'US_TEAM: Increased US quantities +5';

-- Add a new US product
INSERT INTO sales VALUES (101, 'US', 'Monitor', 400.00, 8);
SELECT 'US_TEAM: Added Monitor product';

SELECT 'US_TEAM: Final state:';
SELECT region, product, amount, quantity FROM sales WHERE region = 'US' ORDER BY id;
SELECT 'US_TEAM: Total US amount = ' || SUM(amount)::VARCHAR FROM sales WHERE region = 'US';
