-- Worker 3: DROP SCHEMA CASCADE operations on dev branch
ATTACH 'ducklake:metadata=postgres:host=localhost port=5433 dbname=parallel_same_drop' AS t (DATA_PATH '/tmp/parallel_same_drop');
USE t;
CALL ducklake_use_branch('t', 'dev');

SELECT 'W3: Starting DROP SCHEMA operations' as msg;

SELECT 'W3: Dropping drop_me_1 CASCADE...' as msg;
DROP SCHEMA drop_me_1 CASCADE;

SELECT 'W3: Dropping drop_me_2 CASCADE...' as msg;
DROP SCHEMA drop_me_2 CASCADE;

SELECT 'W3: Dropping drop_me_3 CASCADE...' as msg;
DROP SCHEMA drop_me_3 CASCADE;

SELECT 'W3: Remaining schemas:' as msg;
SELECT schema_name FROM information_schema.schemata
WHERE catalog_name = 't' AND schema_name NOT IN ('information_schema', 'pg_catalog')
ORDER BY schema_name;

-- Verify keep_me still works
SELECT 'W3: keep_me.important data:' as msg;
SELECT * FROM keep_me.important;

SELECT 'W3: Complete' as msg;
