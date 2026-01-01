-- Feature X: Adds feature X related settings
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_merge_sim host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_merge_sim');
USE t;

CALL ducklake_use_branch('t', 'feature_x');
SELECT * FROM ducklake_current_branch('t');

SELECT 'FEATURE_X: Adding feature X settings...';

-- Add feature X config
INSERT INTO config VALUES
    ('feature_x.enabled', 'true', 'features', 2),
    ('feature_x.timeout', '30', 'features', 2),
    ('feature_x.max_retries', '3', 'features', 2);

-- Update version
UPDATE config SET value = '1.1.0-x', version = 2 WHERE key = 'app.version';

-- Modify existing setting
UPDATE config SET value = 'DEBUG', version = 2 WHERE key = 'log.level';

SELECT 'FEATURE_X: Final state:';
SELECT * FROM config ORDER BY category, key;
