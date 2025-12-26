-- Minimal test to debug Test 12 issue
-- The problem: After UPDATE on a branch, WHERE clause finds 0 rows

LOAD 'build/release/extension/ducklake/ducklake.duckdb_extension';

-- Clean start
.shell rm -rf test12_debug.ducklake test12_debug.ducklake.files

ATTACH 'ducklake:test12_debug.ducklake' AS test_lake;
USE test_lake;

-- Create departments table on main
CREATE TABLE test_lake.main.departments (id INT, name VARCHAR, budget DECIMAL(15,2));
INSERT INTO test_lake.main.departments VALUES 
    (1, 'Engineering', 1000000.00),
    (2, 'Sales', 500000.00);

SELECT 'After initial insert on main:' as step;
SELECT * FROM test_lake.main.departments ORDER BY id;

-- Create branch
CALL ducklake_create_branch('test_lake', 'budget_branch');
CALL ducklake_use_branch('test_lake', 'budget_branch');

SELECT 'After switching to budget_branch:' as step;
SELECT * FROM test_lake.main.departments ORDER BY id;

-- Perform UPDATE
UPDATE test_lake.main.departments SET budget = 1500000.00 WHERE name = 'Engineering';

SELECT 'After UPDATE on budget_branch:' as step;
SELECT * FROM test_lake.main.departments ORDER BY id;

-- Now try the failing query
SELECT 'The failing query - looking for Engineering:' as step;
SELECT * FROM test_lake.main.departments WHERE name = 'Engineering';

-- Check metadata
SELECT 'Data files:' as step;
SELECT branch_id, data_file_id, table_id, begin_snapshot, end_snapshot
FROM "__ducklake_metadata_test_lake".main.ducklake_data_file 
ORDER BY branch_id, data_file_id;

SELECT 'Delete files:' as step;
SELECT branch_id, delete_file_id, data_file_id, data_file_branch_id, begin_snapshot, end_snapshot
FROM "__ducklake_metadata_test_lake".main.ducklake_delete_file 
ORDER BY branch_id, delete_file_id;

SELECT 'Branch lineage:' as step;
SELECT * FROM "__ducklake_metadata_test_lake".main.ducklake_branch_lineage 
ORDER BY branch_id, ancestor_branch_id;

SELECT 'Branches:' as step;
SELECT branch_id, branch_name, parent_branch_id, fork_snapshot_id, head_snapshot_id
FROM "__ducklake_metadata_test_lake".main.ducklake_branch 
ORDER BY branch_id;
