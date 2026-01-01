-- Global: Updates across ALL partitions
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_high_card host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_high_card');
USE t;

CALL ducklake_use_branch('t', 'global_ops');
SELECT 'GLOBAL: Operating on ALL 15 countries';

-- Global 10% increase
UPDATE events SET value = value + (value * 0.10);
SELECT 'GLOBAL: Applied 10% increase to all';

-- Change all views to impressions
UPDATE events SET event_type = 'impression' WHERE event_type = 'view';
SELECT 'GLOBAL: Renamed view -> impression';

SELECT 'GLOBAL: Final state:';
SELECT country, COUNT(*) as cnt FROM events GROUP BY country ORDER BY country;
SELECT 'GLOBAL: Total value = ' || SUM(value)::INT::VARCHAR FROM events;
