-- Americas: Operates on US, CA, MX, BR, AR
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_high_card host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_high_card');
USE t;

CALL ducklake_use_branch('t', 'americas');
SELECT 'AMERICAS: Operating on US, CA, MX, BR, AR';

UPDATE events SET value = value * 2 WHERE country IN ('US', 'CA', 'MX', 'BR', 'AR');
INSERT INTO events VALUES (101, 'US', 'purchase', 500), (102, 'BR', 'purchase', 400);
DELETE FROM events WHERE country = 'AR' AND event_type = 'view';

SELECT 'AMERICAS: Final state:';
SELECT country, COUNT(*) as cnt, SUM(value) as total FROM events WHERE country IN ('US', 'CA', 'MX', 'BR', 'AR') GROUP BY country ORDER BY country;
