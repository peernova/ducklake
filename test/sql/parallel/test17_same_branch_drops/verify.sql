-- Verify final state after parallel DROP operations on same branch
ATTACH 'ducklake:metadata=postgres:host=localhost port=5433 dbname=parallel_same_drop' AS t (DATA_PATH '/tmp/parallel_same_drop');
USE t;

SELECT '=== VERIFICATION: SAME BRANCH PARALLEL DROPS ===' as msg;

-- ============================================
-- MAIN BRANCH: Should have ALL original objects
-- ============================================
CALL ducklake_use_branch('t', 'main');
SELECT '--- MAIN BRANCH (should be unchanged) ---' as msg;

SELECT 'Main schemas:' as msg;
SELECT schema_name FROM information_schema.schemata
WHERE catalog_name = 't' AND schema_name NOT IN ('information_schema', 'pg_catalog')
ORDER BY schema_name;

SELECT 'Main: tables_only tables:' as msg;
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'tables_only' AND table_catalog = 't'
ORDER BY table_name;

SELECT 'Main: views_schema views:' as msg;
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'views_schema' AND table_type = 'VIEW' AND table_catalog = 't'
ORDER BY table_name;

SELECT 'Main: drop_me_1 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES' ELSE 'NO' END as check
FROM information_schema.schemata WHERE schema_name = 'drop_me_1' AND catalog_name = 't';

-- ============================================
-- DEV BRANCH: Should reflect all parallel drops
-- ============================================
CALL ducklake_use_branch('t', 'dev');
SELECT '--- DEV BRANCH (all parallel drops applied) ---' as msg;

SELECT 'Dev schemas:' as msg;
SELECT schema_name FROM information_schema.schemata
WHERE catalog_name = 't' AND schema_name NOT IN ('information_schema', 'pg_catalog')
ORDER BY schema_name;

-- Worker 1: t1, t2, t3 dropped, t4 kept
SELECT 'Dev: tables_only.t1 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - WRONG!' ELSE 'NO - correct' END as check
FROM information_schema.tables WHERE table_schema = 'tables_only' AND table_name = 't1' AND table_catalog = 't';

SELECT 'Dev: tables_only.t4 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - correct' ELSE 'NO - WRONG!' END as check
FROM information_schema.tables WHERE table_schema = 'tables_only' AND table_name = 't4' AND table_catalog = 't';

-- Worker 2: v1, v2 dropped, v3 and v_all kept
SELECT 'Dev: views_schema.v1 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - WRONG!' ELSE 'NO - correct' END as check
FROM information_schema.tables WHERE table_schema = 'views_schema' AND table_name = 'v1' AND table_catalog = 't';

SELECT 'Dev: views_schema.v3 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - correct' ELSE 'NO - WRONG!' END as check
FROM information_schema.tables WHERE table_schema = 'views_schema' AND table_name = 'v3' AND table_catalog = 't';

-- Worker 3: drop_me_1, drop_me_2, drop_me_3 schemas dropped
SELECT 'Dev: drop_me_1 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - WRONG!' ELSE 'NO - correct' END as check
FROM information_schema.schemata WHERE schema_name = 'drop_me_1' AND catalog_name = 't';

SELECT 'Dev: drop_me_2 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - WRONG!' ELSE 'NO - correct' END as check
FROM information_schema.schemata WHERE schema_name = 'drop_me_2' AND catalog_name = 't';

SELECT 'Dev: drop_me_3 exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - WRONG!' ELSE 'NO - correct' END as check
FROM information_schema.schemata WHERE schema_name = 'drop_me_3' AND catalog_name = 't';

-- keep_me should survive
SELECT 'Dev: keep_me exists: ' || CASE WHEN COUNT(*) > 0 THEN 'YES - correct' ELSE 'NO - WRONG!' END as check
FROM information_schema.schemata WHERE schema_name = 'keep_me' AND catalog_name = 't';

-- Verify data in surviving objects
SELECT 'Dev: t4 data:' as msg;
SELECT * FROM tables_only.t4;

SELECT 'Dev: v3 data:' as msg;
SELECT * FROM views_schema.v3;

SELECT 'Dev: keep_me.important data:' as msg;
SELECT * FROM keep_me.important;

-- ============================================
-- TIME TRAVEL: Access dropped objects from main
-- ============================================
SELECT '--- TIME TRAVEL FROM DEV TO MAIN ---' as msg;

SELECT 'From dev, access main t1 via time travel:' as msg;
SELECT * FROM tables_only.t1 AT (BRANCH => 'main');

SELECT 'From dev, access main v1 via time travel:' as msg;
SELECT * FROM views_schema.v1 AT (BRANCH => 'main');

SELECT 'From dev, access main drop_me_1.data via time travel:' as msg;
SELECT * FROM drop_me_1.data AT (BRANCH => 'main');

SELECT '=== ALL CHECKS COMPLETE ===' as msg;
