-- Setup: Config table for merge simulation
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_merge_sim host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_merge_sim');
USE t;

CREATE TABLE config (
    key VARCHAR,
    value VARCHAR,
    category VARCHAR,
    version INT
);

-- Initial config on main
INSERT INTO config VALUES
    ('app.name', 'MyApp', 'general', 1),
    ('app.version', '1.0.0', 'general', 1),
    ('db.host', 'localhost', 'database', 1),
    ('db.port', '5432', 'database', 1),
    ('cache.enabled', 'true', 'cache', 1),
    ('log.level', 'INFO', 'logging', 1);

SELECT 'Main: Created config with ' || COUNT(*)::VARCHAR || ' settings' FROM config;

-- Create feature branches
CALL ducklake_create_branch('t', 'feature_x');
CALL ducklake_create_branch('t', 'feature_y');
CALL ducklake_create_branch('t', 'hotfix');
CALL ducklake_create_branch('t', 'main_cont');

SELECT 'Created 4 branches for merge simulation' as msg;
