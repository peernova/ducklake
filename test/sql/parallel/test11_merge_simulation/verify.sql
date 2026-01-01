-- Verify: All branches have divergent but valid states
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_merge_sim host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_merge_sim');
USE t;

SELECT '=== MERGE SIMULATION VERIFICATION ===' as msg;

SELECT 'Main: ' || COUNT(*)::VARCHAR || ' settings (expected: 6)' FROM config AT (BRANCH => 'main');
SELECT 'Feature_X: ' || COUNT(*)::VARCHAR || ' settings (expected: 9 = 6+3)' FROM config AT (BRANCH => 'feature_x');
SELECT 'Feature_Y: ' || COUNT(*)::VARCHAR || ' settings (expected: 9 = 6+3)' FROM config AT (BRANCH => 'feature_y');
SELECT 'Hotfix: ' || COUNT(*)::VARCHAR || ' settings (expected: 7 = 6+1)' FROM config AT (BRANCH => 'hotfix');
SELECT 'Main_cont: ' || COUNT(*)::VARCHAR || ' settings (expected: 9 = 6-1+4)' FROM config AT (BRANCH => 'main_cont');

SELECT '--- app.version across branches (shows divergence) ---' as msg;
SELECT 'Main: ' || value FROM config AT (BRANCH => 'main') WHERE key = 'app.version';
SELECT 'Feature_X: ' || value FROM config AT (BRANCH => 'feature_x') WHERE key = 'app.version';
SELECT 'Feature_Y: ' || value FROM config AT (BRANCH => 'feature_y') WHERE key = 'app.version';
SELECT 'Hotfix: ' || value FROM config AT (BRANCH => 'hotfix') WHERE key = 'app.version';
SELECT 'Main_cont: ' || value FROM config AT (BRANCH => 'main_cont') WHERE key = 'app.version';

SELECT '--- log.level across branches ---' as msg;
SELECT 'Main: ' || value FROM config AT (BRANCH => 'main') WHERE key = 'log.level';
SELECT 'Feature_X: ' || value FROM config AT (BRANCH => 'feature_x') WHERE key = 'log.level';
SELECT 'Hotfix: ' || value FROM config AT (BRANCH => 'hotfix') WHERE key = 'log.level';

SELECT '--- Unique settings per branch ---' as msg;
SELECT 'Feature_X has feature_x.enabled: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END FROM config AT (BRANCH => 'feature_x') WHERE key = 'feature_x.enabled';
SELECT 'Feature_Y has feature_y.endpoint: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END FROM config AT (BRANCH => 'feature_y') WHERE key = 'feature_y.endpoint';
SELECT 'Hotfix has security.ssl: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END FROM config AT (BRANCH => 'hotfix') WHERE key = 'security.ssl';
SELECT 'Main_cont has cache.type: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END FROM config AT (BRANCH => 'main_cont') WHERE key = 'cache.type';

SELECT '=== ISOLATION CHECKS ===' as msg;
SELECT 'Main unchanged (version=1.0.0): ' || CASE WHEN value = '1.0.0' THEN 'YES' ELSE 'NO - ' || value END FROM config AT (BRANCH => 'main') WHERE key = 'app.version';
SELECT 'Main has NO feature_x settings: ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO' END FROM config AT (BRANCH => 'main') WHERE key LIKE 'feature_x%';
SELECT 'Feature_X has NO feature_y settings: ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO' END FROM config AT (BRANCH => 'feature_x') WHERE key LIKE 'feature_y%';
SELECT 'Main_cont deleted cache.enabled: ' || CASE WHEN COUNT(*) = 0 THEN 'YES' ELSE 'NO' END FROM config AT (BRANCH => 'main_cont') WHERE key = 'cache.enabled';
SELECT 'Feature_X still has cache.enabled: ' || CASE WHEN COUNT(*) = 1 THEN 'YES' ELSE 'NO' END FROM config AT (BRANCH => 'feature_x') WHERE key = 'cache.enabled';
