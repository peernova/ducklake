


-- Test without branching - just multiple files on main branch
ATTACH 'ducklake:test_no_branch.ducklake' AS test_lake;

CREATE TABLE test_lake.main.departments (id INT, name VARCHAR, budget DECIMAL(15,2));

-- First INSERT creates file 1
INSERT INTO test_lake.main.departments VALUES (1, 'Engineering', 1000000.00);

-- Second INSERT creates file 2  
INSERT INTO test_lake.main.departments VALUES (2, 'Sales', 500000.00);

SELECT * FROM test_lake.main.departments ORDER BY id;

