-- New Regions: Adds new partition values
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_high_card host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_high_card');
USE t;

CALL ducklake_use_branch('t', 'new_regions');
SELECT 'NEW_REGIONS: Adding 5 new countries';

-- Add 5 new countries (new partition values)
INSERT INTO events VALUES
    (401, 'ZA', 'click', 50), (402, 'ZA', 'view', 25),
    (403, 'NG', 'click', 45), (404, 'NG', 'view', 22),
    (405, 'EG', 'click', 40), (406, 'EG', 'view', 20),
    (407, 'NZ', 'click', 35), (408, 'NZ', 'view', 17),
    (409, 'SG', 'click', 60), (410, 'SG', 'view', 30);

SELECT 'NEW_REGIONS: Final state:';
SELECT country, COUNT(*) as cnt FROM events GROUP BY country ORDER BY country;
SELECT 'NEW_REGIONS: Now have ' || COUNT(DISTINCT country)::VARCHAR || ' countries' FROM events;
