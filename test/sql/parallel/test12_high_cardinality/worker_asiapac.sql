-- AsiaPac: Operates on JP, CN, IN, AU, KR
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_high_card host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_high_card');
USE t;

CALL ducklake_use_branch('t', 'asiapac');
SELECT 'ASIAPAC: Operating on JP, CN, IN, AU, KR';

UPDATE events SET value = value * 1.5 WHERE country IN ('JP', 'CN', 'IN', 'AU', 'KR');
INSERT INTO events VALUES (301, 'JP', 'premium', 2000), (302, 'CN', 'premium', 2500), (303, 'IN', 'premium', 1800);
DELETE FROM events WHERE country = 'AU' AND event_type = 'click';

SELECT 'ASIAPAC: Final state:';
SELECT country, COUNT(*) as cnt, SUM(value)::INT as total FROM events WHERE country IN ('JP', 'CN', 'IN', 'AU', 'KR') GROUP BY country ORDER BY country;
