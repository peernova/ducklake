-- Hotfix: Fixes critical settings
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_merge_sim host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_merge_sim');
USE t;

CALL ducklake_use_branch('t', 'hotfix');
SELECT * FROM ducklake_current_branch('t');

SELECT 'HOTFIX: Applying critical fixes...';

-- Update version for hotfix
UPDATE config SET value = '1.0.1', version = 2 WHERE key = 'app.version';

-- Fix database settings (critical bug fix)
UPDATE config SET value = 'prod-db.example.com', version = 2 WHERE key = 'db.host';
UPDATE config SET value = '5433', version = 2 WHERE key = 'db.port';

-- Add security setting
INSERT INTO config VALUES ('security.ssl', 'required', 'security', 2);

-- Set log level to ERROR for production
UPDATE config SET value = 'ERROR', version = 2 WHERE key = 'log.level';

SELECT 'HOTFIX: Final state:';
SELECT * FROM config ORDER BY category, key;
