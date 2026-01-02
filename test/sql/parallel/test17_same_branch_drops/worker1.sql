-- Worker 1: DROP TABLE operations on dev branch
ATTACH 'ducklake:metadata=postgres:host=localhost port=5433 dbname=parallel_same_drop' AS t (DATA_PATH '/tmp/parallel_same_drop');
USE t;
CALL ducklake_use_branch('t', 'dev');

SELECT 'W1: Starting DROP TABLE operations' as msg;

-- Small delay to interleave with other workers
SELECT 'W1: Dropping t1...' as msg;
DROP TABLE tables_only.t1;

SELECT 'W1: Dropping t2...' as msg;
DROP TABLE tables_only.t2;

SELECT 'W1: Dropping t3...' as msg;
DROP TABLE tables_only.t3;

-- Keep t4 to verify partial drops work
SELECT 'W1: Kept t4, remaining tables:' as msg;
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'tables_only' AND table_catalog = 't'
ORDER BY table_name;

SELECT 'W1: Complete' as msg;
