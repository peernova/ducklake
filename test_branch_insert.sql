LOAD 'build/release/extension/ducklake/ducklake.duckdb_extension';

-- Test branching with INSERTs only
ATTACH 'ducklake:test_branch_insert.ducklake' AS test_lake;

CREATE TABLE test_lake.main.departments (id INT, name VARCHAR, budget DECIMAL(15,2));
INSERT INTO test_lake.main.departments VALUES 
    (1, 'Engineering', 1000000.00),
    (2, 'Sales', 500000.00),
    (3, 'Marketing', 300000.00);

CALL ducklake_create_branch('test_lake', 'dev_branch');
CALL ducklake_use_branch('test_lake', 'dev_branch');

-- Just INSERT on child branch, no UPDATE
INSERT INTO test_lake.main.departments VALUES (4, 'HR', 200000.00);

SELECT 'Query on dev_branch:' as info;
SELECT * FROM test_lake.main.departments ORDER BY id;


SELECT '###########3' as sdf;

CALL ducklake_use_branch('test_lake', 'main');
SELECT * FROM test_lake.main.departments ORDER BY id;



