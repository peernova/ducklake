-- Main Continuation: Development continues on main
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_merge_sim host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_merge_sim');
USE t;

CALL ducklake_use_branch('t', 'main_cont');
SELECT * FROM ducklake_current_branch('t');

SELECT 'MAIN_CONT: Continuing main development...';

-- Add new general settings
INSERT INTO config VALUES
    ('app.description', 'My Application', 'general', 2),
    ('app.maintainer', 'team@example.com', 'general', 2);

-- Update version on main
UPDATE config SET value = '1.2.0-dev', version = 2 WHERE key = 'app.version';

-- Delete deprecated setting
DELETE FROM config WHERE key = 'cache.enabled';

-- Add new cache settings
INSERT INTO config VALUES
    ('cache.type', 'memory', 'cache', 2),
    ('cache.ttl', '3600', 'cache', 2);

SELECT 'MAIN_CONT: Final state:';
SELECT * FROM config ORDER BY category, key;
