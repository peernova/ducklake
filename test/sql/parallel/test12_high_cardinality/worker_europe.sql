-- Europe: Operates on UK, DE, FR, IT, ES
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_high_card host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_high_card');
USE t;

CALL ducklake_use_branch('t', 'europe');
SELECT 'EUROPE: Operating on UK, DE, FR, IT, ES';

UPDATE events SET value = value + 50 WHERE country IN ('UK', 'DE', 'FR', 'IT', 'ES');
INSERT INTO events VALUES (201, 'DE', 'subscribe', 1000), (202, 'UK', 'subscribe', 900);
DELETE FROM events WHERE country = 'ES';

SELECT 'EUROPE: Final state:';
SELECT country, COUNT(*) as cnt, SUM(value) as total FROM events WHERE country IN ('UK', 'DE', 'FR', 'IT', 'ES') GROUP BY country ORDER BY country;
