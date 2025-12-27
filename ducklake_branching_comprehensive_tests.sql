-- ============================================
-- DUCKLAKE BRANCHING COMPREHENSIVE TEST SUITE
-- ============================================
-- Run with:
-- ./build/release/duckdb -unsigned < ducklake_branching_comprehensive_tests.sql
-- ============================================

-- Load the extension
LOAD 'build/release/extension/ducklake/ducklake.duckdb_extension';

-- ============================================
-- SETUP: Create Fresh DuckLake Catalog
-- ============================================
ATTACH 'ducklake:test_branch.ducklake' AS test_lake;
USE test_lake;





-- ============================================================================
-- TEST 1: Basic Branch Isolation for Data
-- ============================================================================
SELECT '=== TEST 1: Basic Branch Isolation for Data ===' as test_section;

-- Create table and insert data on main branch
CREATE TABLE test_lake.main.employees (id INT, name VARCHAR, department VARCHAR);
INSERT INTO test_lake.main.employees VALUES 
    (1, 'Alice', 'Engineering'),
    (2, 'Bob', 'Sales'),
    (3, 'Charlie', 'Marketing');

SELECT 'Main branch initial count' as test, COUNT(*) as result, 3 as expected FROM test_lake.main.employees;

-- Create dev branch
CALL ducklake_create_branch('test_lake', 'dev_branch');

-- Switch to dev branch and add data
CALL ducklake_use_branch('test_lake', 'dev_branch');
INSERT INTO test_lake.main.employees VALUES 
    (4, 'Diana', 'Engineering'),
    (5, 'Eve', 'Sales');

SELECT 'Dev branch count after insert' as test, COUNT(*) as result, 5 as expected FROM test_lake.main.employees;

-- Switch back to main - should NOT see Diana and Eve
CALL ducklake_use_branch('test_lake', 'main');
SELECT 'Main branch count (should NOT include dev data)' as test, COUNT(*) as result, 3 as expected FROM test_lake.main.employees;

-- Verify actual data on main
SELECT 'Main branch employees:' as info;
SELECT * FROM test_lake.main.employees ORDER BY id;

-- ============================================================================
-- TEST 2: Schema Changes Isolation (ADD COLUMN)
-- ============================================================================
SELECT '=== TEST 2: Schema Changes Isolation (ADD COLUMN) ===' as test_section;

-- Switch to dev branch and add a column
CALL ducklake_use_branch('test_lake', 'dev_branch');
ALTER TABLE test_lake.main.employees ADD COLUMN salary INT DEFAULT 50000;

-- Update salary on dev branch
UPDATE test_lake.main.employees SET salary = 75000 WHERE name = 'Alice';
UPDATE test_lake.main.employees SET salary = 65000 WHERE name = 'Bob';

SELECT 'Dev branch with salary column:' as info;
SELECT * FROM test_lake.main.employees ORDER BY id;

-- Switch back to main - salary column should NOT exist
CALL ducklake_use_branch('test_lake', 'main');
SELECT 'Main branch columns (should NOT have salary):' as info;
SELECT column_name FROM (DESCRIBE test_lake.main.employees);

-- ============================================================================
-- TEST 3: New Table Isolation
-- ============================================================================
SELECT '=== TEST 3: New Table Isolation ===' as test_section;

-- Switch to dev branch and create a new table
CALL ducklake_use_branch('test_lake', 'dev_branch');
CREATE TABLE test_lake.main.projects (id INT, name VARCHAR, lead_id INT);
INSERT INTO test_lake.main.projects VALUES 
    (1, 'Project Alpha', 1),
    (2, 'Project Beta', 4);

SELECT 'Dev branch projects table:' as info;
SELECT * FROM test_lake.main.projects ORDER BY id;

-- Switch back to main - projects table should NOT exist
CALL ducklake_use_branch('test_lake', 'main');
SELECT 'Tables on main branch:' as info;
SELECT table_name FROM duckdb_tables() WHERE database_name = 'test_lake' AND schema_name = 'main';

-- ============================================================================
-- TEST 4: Multiple Branches from Same Parent
-- ============================================================================
SELECT '=== TEST 4: Multiple Branches from Same Parent ===' as test_section;

-- Create another branch from main
CALL ducklake_create_branch('test_lake', 'feature_branch');
CALL ducklake_use_branch('test_lake', 'feature_branch');

-- Add different data on feature branch
INSERT INTO test_lake.main.employees VALUES (6, 'Frank', 'HR');

SELECT 'Feature branch count' as test, COUNT(*) as result, 4 as expected FROM test_lake.main.employees;

-- Verify all three branches have correct counts
CALL ducklake_use_branch('test_lake', 'main');
SELECT 'Main branch count' as test, COUNT(*) as result, 3 as expected FROM test_lake.main.employees;

CALL ducklake_use_branch('test_lake', 'dev_branch');
SELECT 'Dev branch count' as test, COUNT(*) as result, 5 as expected FROM test_lake.main.employees;

CALL ducklake_use_branch('test_lake', 'feature_branch');
SELECT 'Feature branch count' as test, COUNT(*) as result, 4 as expected FROM test_lake.main.employees;

-- ============================================================================
-- TEST 5: Delete Isolation
-- ============================================================================
SELECT '=== TEST 5: Delete Isolation ===' as test_section;

-- On feature branch, delete a record
CALL ducklake_use_branch('test_lake', 'feature_branch');
DELETE FROM test_lake.main.employees WHERE name = 'Charlie';

SELECT 'Feature branch after delete' as test, COUNT(*) as result, 3 as expected FROM test_lake.main.employees;
SELECT 'Feature branch employees after delete:' as info;
SELECT * FROM test_lake.main.employees ORDER BY id;

-- Main should still have Charlie
CALL ducklake_use_branch('test_lake', 'main');
SELECT 'Main branch still has Charlie' as test, COUNT(*) as result, 3 as expected FROM test_lake.main.employees;
SELECT 'Charlie exists on main' as test, COUNT(*) as result, 1 as expected FROM test_lake.main.employees WHERE name = 'Charlie';

-- ============================================================================
-- TEST 6: Update Isolation
-- ============================================================================
SELECT '=== TEST 6: Update Isolation ===' as test_section;

-- On dev branch, update a record
CALL ducklake_use_branch('test_lake', 'dev_branch');
UPDATE test_lake.main.employees SET department = 'Executive' WHERE name = 'Alice';

SELECT 'Dev branch Alice department' as test, department as result, 'Executive' as expected 
FROM test_lake.main.employees WHERE name = 'Alice';

-- Main should still have Alice in Engineering
CALL ducklake_use_branch('test_lake', 'main');
SELECT 'Main branch Alice department' as test, department as result, 'Engineering' as expected 
FROM test_lake.main.employees WHERE name = 'Alice';

-- ============================================================================
-- TEST 7: Branch AT Clause (Time Travel within Branch)
-- ============================================================================
SELECT '=== TEST 7: Branch AT Clause ===' as test_section;

-- Add more data to main
INSERT INTO test_lake.main.employees VALUES (7, 'Grace', 'Finance');
SELECT 'Main after adding Grace' as test, COUNT(*) as result, 4 as expected FROM test_lake.main.employees;

-- Query main at earlier version (before Grace)
SELECT 'Main at version 2 (before Grace)' as test, COUNT(*) as result, 3 as expected 
FROM test_lake.main.employees AT (VERSION => 2);

-- ============================================================================
-- TEST 8: Verify Metadata State
-- ============================================================================
SELECT '=== TEST 8: Metadata State ===' as test_section;

SELECT 'Branches:' as info;
SELECT branch_id, branch_name, parent_branch_id, fork_snapshot_id, head_snapshot_id 
FROM "__ducklake_metadata_test_lake".main.ducklake_branch 
ORDER BY branch_id;

SELECT 'Branch Lineage:' as info;
SELECT * FROM "__ducklake_metadata_test_lake".main.ducklake_branch_lineage 
ORDER BY branch_id, ancestor_branch_id;

SELECT 'Data Files:' as info;
SELECT branch_id, data_file_id, table_id, begin_snapshot, end_snapshot
FROM "__ducklake_metadata_test_lake".main.ducklake_data_file 
ORDER BY branch_id, data_file_id;

-- ============================================================================
-- TEST 9: Nested Branch (Branch from Branch)
-- ============================================================================
SELECT '=== TEST 9: Nested Branch (Branch from Branch) ===' as test_section;

-- Create a branch from dev_branch
CALL ducklake_use_branch('test_lake', 'dev_branch');
CALL ducklake_create_branch('test_lake', 'dev_feature');
CALL ducklake_use_branch('test_lake', 'dev_feature');

-- Add data on nested branch (dev_branch has salary column, so include it)
INSERT INTO test_lake.main.employees VALUES (8, 'Henry', 'Legal', 55000);

SELECT 'Dev_feature branch count (inherits from dev)' as test, COUNT(*) as result, 6 as expected FROM test_lake.main.employees;

-- Verify dev_branch doesn't see Henry
CALL ducklake_use_branch('test_lake', 'dev_branch');
SELECT 'Dev branch count (should NOT have Henry)' as test, COUNT(*) as result, 5 as expected FROM test_lake.main.employees;

-- Verify main doesn't see any of the branch data
CALL ducklake_use_branch('test_lake', 'main');
SELECT 'Main branch count (original + Grace)' as test, COUNT(*) as result, 4 as expected FROM test_lake.main.employees;

-- ============================================================================
-- TEST 10: Verify Column Visibility
-- ============================================================================
SELECT '=== TEST 10: Column Visibility ===' as test_section;

-- Dev branch should have salary column
CALL ducklake_use_branch('test_lake', 'dev_branch');
SELECT 'Dev branch column count' as test, COUNT(*) as result, 4 as expected 
FROM (DESCRIBE test_lake.main.employees);

-- Main branch should NOT have salary column
CALL ducklake_use_branch('test_lake', 'main');
SELECT 'Main branch column count' as test, COUNT(*) as result, 3 as expected 
FROM (DESCRIBE test_lake.main.employees);

-- ============================================================================
-- TEST 11: Deep Branch Hierarchy (3+ levels)
-- ============================================================================
SELECT '=== TEST 11: Deep Branch Hierarchy ===' as test_section;

-- Create a 4-level hierarchy: main -> dev_branch -> dev_feature -> dev_feature_sub
CALL ducklake_use_branch('test_lake', 'dev_feature');
CALL ducklake_create_branch('test_lake', 'dev_feature_sub');
CALL ducklake_use_branch('test_lake', 'dev_feature_sub');

-- Should inherit all data from dev_feature (which inherited from dev_branch)
SELECT 'dev_feature_sub inherits from dev_feature' as test, COUNT(*) as result, 6 as expected FROM test_lake.main.employees;

-- Add unique data to this deepest branch
INSERT INTO test_lake.main.employees VALUES (9, 'Ivy', 'Research', 80000);
SELECT 'dev_feature_sub after insert' as test, COUNT(*) as result, 7 as expected FROM test_lake.main.employees;

-- Verify parent branches don't see this data
CALL ducklake_use_branch('test_lake', 'dev_feature');
SELECT 'dev_feature unchanged' as test, COUNT(*) as result, 6 as expected FROM test_lake.main.employees;

CALL ducklake_use_branch('test_lake', 'dev_branch');
SELECT 'dev_branch unchanged' as test, COUNT(*) as result, 5 as expected FROM test_lake.main.employees;

CALL ducklake_use_branch('test_lake', 'main');
SELECT 'main unchanged' as test, COUNT(*) as result, 4 as expected FROM test_lake.main.employees;

-- ============================================================================
-- TEST 12: Multiple Tables with Different Branch States
-- ============================================================================
SELECT '=== TEST 12: Multiple Tables with Different Branch States ===' as test_section;

-- Create a departments table on main
CALL ducklake_use_branch('test_lake', 'main');
CREATE TABLE test_lake.main.departments (id INT, name VARCHAR, budget DECIMAL(15,2));
INSERT INTO test_lake.main.departments VALUES 
    (1, 'Engineering', 1000000.00),
    (2, 'Sales', 500000.00),
    (3, 'Marketing', 300000.00);

SELECT 'Main departments count' as test, COUNT(*) as result, 3 as expected FROM test_lake.main.departments;

-- Create a new branch and modify departments differently
CALL ducklake_create_branch('test_lake', 'budget_branch');
CALL ducklake_use_branch('test_lake', 'budget_branch');

-- Add new department and modify budget
INSERT INTO test_lake.main.departments VALUES (4, 'HR', 200000.00);
UPDATE test_lake.main.departments SET budget = 1500000.00 WHERE name = 'Engineering';

SELECT '=== not main branch ===' as test_section;
SELECT 'budget_branch departments count' as test, COUNT(*) as result, 4 as expected FROM test_lake.main.departments;
SELECT 'budget_branch Engineering budget' as test, budget as result, 1500000.00 as expected 
FROM test_lake.main.departments WHERE name = 'Engineering';

-- Verify main is unchanged
CALL ducklake_use_branch('test_lake', 'main');
SELECT 'Main departments still 3' as test, COUNT(*) as result, 3 as expected FROM test_lake.main.departments;
SELECT 'Main Engineering budget unchanged' as test, budget as result, 1000000.00 as expected 
FROM test_lake.main.departments WHERE name = 'Engineering';




-- Check what files exist for departments
SELECT 'Data files:' as info;
SELECT branch_id, data_file_id, begin_snapshot, end_snapshot, record_count, path
FROM "__ducklake_metadata_test_lake".main.ducklake_data_file 
ORDER BY branch_id, data_file_id;

SELECT 'Delete files:' as info;
SELECT branch_id, delete_file_id, begin_snapshot, end_snapshot, data_file_id, data_file_branch_id
FROM "__ducklake_metadata_test_lake".main.ducklake_delete_file;

SELECT 'Branch lineage for budget_branch (branch_id=5):' as info;
SELECT * FROM "__ducklake_metadata_test_lake".main.ducklake_branch_lineage WHERE branch_id = 5;

SELECT 'Current branch info:' as info;
SELECT * FROM "__ducklake_metadata_test_lake".main.ducklake_branch;


SELECT branch_id, table_id, table_name, begin_snapshot 
FROM "__ducklake_metadata_test_lake".main.ducklake_table 
WHERE table_name = 'departments'
ORDER BY branch_id, table_id;

SELECT branch_id, data_file_id, table_id, begin_snapshot, record_count, path
FROM "__ducklake_metadata_test_lake".main.ducklake_data_file 
WHERE table_id = 2
ORDER BY branch_id, data_file_id;


-- Check delete file details
SELECT * FROM "__ducklake_metadata_test_lake".main.ducklake_delete_file WHERE branch_id = 5;

-- Check what's actually in the delete parquet file
SELECT 'Checking delete file path:' as info;
SELECT path FROM "__ducklake_metadata_test_lake".main.ducklake_delete_file WHERE branch_id = 5;

SELECT 'First make sure we are on budget_branch' as info;

-- First make sure we're on budget_branch
CALL ducklake_use_branch('test_lake', 'budget_branch');
SELECT * FROM ducklake_current_branch('test_lake');

-- Now query ALL departments data
SELECT * FROM test_lake.main.departments ORDER BY id;

-- And specifically the Engineering row
SELECT * FROM test_lake.main.departments WHERE name = 'Engineering';




SELECT 'Disable filter pushdown and see if it works START @@@ ' as info;

-- Disable filter pushdown and see if it works
CALL ducklake_use_branch('test_lake', 'budget_branch');
--SET disabled_optimizers = 'filter_pushdown';
SELECT * FROM test_lake.main.departments WHERE name = 'Engineering';

SELECT 'Disable filter pushdown and see if it works END !!! ' as info;

-- After running comprehensive test, check the file stats
SELECT 'File column stats:' as info;
SELECT branch_id, data_file_id, column_id, min_value, max_value 
FROM "__ducklake_metadata_test_lake".main.ducklake_file_column_stats
WHERE table_id = 2
ORDER BY branch_id, data_file_id, column_id;

-- Check data files for table 2
SELECT 'Data files:' as info;
SELECT branch_id, data_file_id, begin_snapshot, record_count
FROM "__ducklake_metadata_test_lake".main.ducklake_data_file
WHERE table_id = 2
ORDER BY branch_id, data_file_id;

-- Check branch lineage for branch 5
SELECT 'Branch 5 lineage:' as info;
SELECT * FROM "__ducklake_metadata_test_lake".main.ducklake_branch_lineage WHERE branch_id = 5;





