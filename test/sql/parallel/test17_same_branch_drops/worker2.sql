-- Worker 2: DROP VIEW operations on dev branch
ATTACH 'ducklake:metadata=postgres:host=localhost port=5433 dbname=parallel_same_drop' AS t (DATA_PATH '/tmp/parallel_same_drop');
USE t;
CALL ducklake_use_branch('t', 'dev');

SELECT 'W2: Starting DROP VIEW operations' as msg;

SELECT 'W2: Dropping v1...' as msg;
DROP VIEW views_schema.v1;

SELECT 'W2: Dropping v2...' as msg;
DROP VIEW views_schema.v2;

-- Keep v3 and v_all to verify partial drops work
SELECT 'W2: Kept v3 and v_all, remaining views:' as msg;
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'views_schema' AND table_type = 'VIEW' AND table_catalog = 't'
ORDER BY table_name;

-- Verify remaining views still work
SELECT 'W2: v3 data:' as msg;
SELECT * FROM views_schema.v3;

SELECT 'W2: Complete' as msg;
