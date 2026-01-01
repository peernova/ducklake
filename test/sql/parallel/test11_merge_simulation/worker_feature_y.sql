-- Feature Y: Adds feature Y related settings
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_merge_sim host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_merge_sim');
USE t;

CALL ducklake_use_branch('t', 'feature_y');
SELECT * FROM ducklake_current_branch('t');

SELECT 'FEATURE_Y: Adding feature Y settings...';

-- Add feature Y config
INSERT INTO config VALUES
    ('feature_y.enabled', 'false', 'features', 2),
    ('feature_y.api_key', 'secret123', 'features', 2),
    ('feature_y.endpoint', 'https://api.example.com', 'features', 2);

-- Update version (different from X!)
UPDATE config SET value = '1.1.0-y', version = 2 WHERE key = 'app.version';

-- Modify different setting
UPDATE config SET value = 'redis://cache:6379', version = 2 WHERE key = 'cache.enabled';

SELECT 'FEATURE_Y: Final state:';
SELECT * FROM config ORDER BY category, key;
