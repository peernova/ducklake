-- Test 12 with metadata debugging
-- Run with: ./build/release/duckdb -unsigned < test12_metadata_debug.sql

LOAD 'build/release/extension/ducklake/ducklake.duckdb_extension';

-- Fresh start
ATTACH 'ducklake:test12_meta.ducklake' AS test_lake;
USE test_lake;

-- ============================================================================
-- SETUP: Simulate earlier tests to create realistic branch state
-- ============================================================================
SELECT '=== SETUP: Creating branches like comprehensive test ===' as info;

-- Create employees table (like Test 1)
CREATE TABLE test_lake.main.employees (id INT, name VARCHAR, department VARCHAR);
INSERT INTO test_lake.main.employees VALUES 
    (1, 'Alice', 'Engineering'),
    (2, 'Bob', 'Sales'),
    (3, 'Charlie', 'Marketing');

-- Create dev_branch (like Test 1)
CALL ducklake_create_branch('test_lake', 'dev_branch');
CALL ducklake_use_branch('test_lake', 'dev_branch');
INSERT INTO test_lake.main.employees VALUES (4, 'Diana', 'Engineering'), (5, 'Eve', 'Sales');

-- Add salary column on dev_branch (like Test 2)
ALTER TABLE test_lake.main.employees ADD COLUMN salary INT DEFAULT 50000;

-- Create feature_branch from main (like Test 4)
CALL ducklake_use_branch('test_lake', 'main');
CALL ducklake_create_branch('test_lake', 'feature_branch');

-- Add Grace to main (like Test 7)
INSERT INTO test_lake.main.employees VALUES (7, 'Grace', 'Finance');

-- Create dev_feature from dev_branch (like Test 9)  
CALL ducklake_use_branch('test_lake', 'dev_branch');
CALL ducklake_create_branch('test_lake', 'dev_feature');

-- Create dev_feature_sub from dev_feature (like Test 11)
CALL ducklake_use_branch('test_lake', 'dev_feature');
CALL ducklake_create_branch('test_lake', 'dev_feature_sub');

SELECT 'Setup complete - branches created' as info;

-- ============================================================================
-- TEST 12: The actual test
-- ============================================================================
SELECT '=== TEST 12: Multiple Tables with Different Branch States ===' as test_section;

-- Create departments table on main
CALL ducklake_use_branch('test_lake', 'main');
CREATE TABLE test_lake.main.departments (id INT, name VARCHAR, budget DECIMAL(15,2));
INSERT INTO test_lake.main.departments VALUES 
    (1, 'Engineering', 1000000.00),
    (2, 'Sales', 500000.00),
    (3, 'Marketing', 300000.00);

SELECT 'Main departments count' as test, COUNT(*) as result, 3 as expected FROM test_lake.main.departments;

-- Show metadata BEFORE creating budget_branch
SELECT '=== METADATA BEFORE BUDGET_BRANCH ===' as info;
SELECT 'Branches:' as info;
SELECT branch_id, branch_name, parent_branch_id, fork_snapshot_id, head_snapshot_id 
FROM "__ducklake_metadata_test_lake".main.ducklake_branch ORDER BY branch_id;

SELECT 'Data files for departments (table_id from ducklake_table):' as info;
SELECT df.branch_id, df.data_file_id, df.table_id, df.begin_snapshot, df.end_snapshot, df.record_count
FROM "__ducklake_metadata_test_lake".main.ducklake_data_file df
WHERE df.table_id = (SELECT table_id FROM "__ducklake_metadata_test_lake".main.ducklake_table WHERE table_name = 'departments' LIMIT 1)
ORDER BY df.branch_id, df.data_file_id;

-- Create budget_branch and modify departments
CALL ducklake_create_branch('test_lake', 'budget_branch');
CALL ducklake_use_branch('test_lake', 'budget_branch');

SELECT '=== AFTER SWITCHING TO BUDGET_BRANCH ===' as info;
SELECT 'Current branch:' as info;
SELECT * FROM ducklake_current_branch('test_lake');

-- Add new department
INSERT INTO test_lake.main.departments VALUES (4, 'HR', 200000.00);

SELECT '=== AFTER INSERT ON BUDGET_BRANCH ===' as info;
SELECT 'Data files for departments:' as info;
SELECT df.branch_id, df.data_file_id, df.table_id, df.begin_snapshot, df.end_snapshot, df.record_count
FROM "__ducklake_metadata_test_lake".main.ducklake_data_file df
WHERE df.table_id = (SELECT table_id FROM "__ducklake_metadata_test_lake".main.ducklake_table WHERE table_name = 'departments' LIMIT 1)
ORDER BY df.branch_id, df.data_file_id;

-- Now the UPDATE
SELECT '=== ABOUT TO DO UPDATE ===' as info;
UPDATE test_lake.main.departments SET budget = 1500000.00 WHERE name = 'Engineering';

SELECT '=== AFTER UPDATE ON BUDGET_BRANCH ===' as info;

-- Show all metadata
SELECT 'Branches after UPDATE:' as info;
SELECT branch_id, branch_name, parent_branch_id, fork_snapshot_id, head_snapshot_id 
FROM "__ducklake_metadata_test_lake".main.ducklake_branch ORDER BY branch_id;

SELECT 'Branch lineage:' as info;
SELECT * FROM "__ducklake_metadata_test_lake".main.ducklake_branch_lineage ORDER BY branch_id, ancestor_branch_id;

SELECT 'Data files for departments:' as info;
SELECT df.branch_id, df.data_file_id, df.table_id, df.begin_snapshot, df.end_snapshot, df.record_count, df.path
FROM "__ducklake_metadata_test_lake".main.ducklake_data_file df
WHERE df.table_id = (SELECT table_id FROM "__ducklake_metadata_test_lake".main.ducklake_table WHERE table_name = 'departments' LIMIT 1)
ORDER BY df.branch_id, df.data_file_id;

SELECT 'Delete files for departments:' as info;
SELECT del.branch_id, del.delete_file_id, del.table_id, del.data_file_id, del.data_file_branch_id, del.begin_snapshot, del.end_snapshot, del.delete_count
FROM "__ducklake_metadata_test_lake".main.ducklake_delete_file del
WHERE del.table_id = (SELECT table_id FROM "__ducklake_metadata_test_lake".main.ducklake_table WHERE table_name = 'departments' LIMIT 1)
ORDER BY del.branch_id, del.delete_file_id;

SELECT 'Snapshots:' as info;
SELECT branch_id, snapshot_id, schema_version, next_file_id FROM "__ducklake_metadata_test_lake".main.ducklake_snapshot ORDER BY branch_id, snapshot_id;

-- Now test the queries
SELECT '=== QUERY RESULTS ===' as info;
SELECT 'budget_branch departments count' as test, COUNT(*) as result, 4 as expected FROM test_lake.main.departments;

SELECT 'All departments on budget_branch:' as info;
SELECT * FROM test_lake.main.departments ORDER BY id;

SELECT 'budget_branch Engineering budget (THE FAILING QUERY)' as test, budget as result, 1500000.00 as expected 
FROM test_lake.main.departments WHERE name = 'Engineering';

-- Switch back to main to verify isolation
CALL ducklake_use_branch('test_lake', 'main');
SELECT 'Main departments still 3' as test, COUNT(*) as result, 3 as expected FROM test_lake.main.departments;
SELECT 'Main Engineering budget unchanged' as test, budget as result, 1000000.00 as expected 
FROM test_lake.main.departments WHERE name = 'Engineering';
