

LOAD 'build/release/extension/ducklake/ducklake.duckdb_extension';



ATTACH 'ducklake:test_filter.ducklake' AS test_lake;

CREATE TABLE test_lake.main.departments (id INT, name VARCHAR, budget DECIMAL(15,2));
INSERT INTO test_lake.main.departments VALUES 
    (1, 'Engineering', 1000000.00),
    (2, 'Sales', 500000.00);

CALL ducklake_create_branch('test_lake', 'dev_branch');
CALL ducklake_use_branch('test_lake', 'dev_branch');

UPDATE test_lake.main.departments SET budget = 1500000.00 WHERE name = 'Engineering';

-- This should now work without disabling filter_pushdown
SELECT * FROM test_lake.main.departments WHERE name = 'Engineering';
