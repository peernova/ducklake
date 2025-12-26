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

-- ============================================
-- TEST 1: Basic Main Branch Setup
-- ============================================
SELECT '=== TEST 1: Basic Main Branch Setup ===' as test_section;

CREATE TABLE employees (
    id INTEGER,
    name VARCHAR,
    department VARCHAR,
    salary DOUBLE
);

INSERT INTO employees VALUES 
    (1, 'Alice', 'Engineering', 100000),
    (2, 'Bob', 'Engineering', 95000),
    (3, 'Charlie', 'Sales', 80000);

-- Verify main branch data
SELECT 'Main branch employee count' as test, COUNT(*) as result FROM employees;
-- Expected: 3

-- Check branch metadata (adjust schema name based on your setup)
SELECT branch_id, branch_name, head_snapshot_id 
FROM __ducklake_metadata_test_lake."main".ducklake_branch;

-- ============================================
-- TEST 2: Schema Changes on Main Branch
-- ============================================
SELECT '=== TEST 2: Schema Changes on Main Branch ===' as test_section;

-- Add a new column
ALTER TABLE employees ADD COLUMN hire_date DATE;

-- Update existing records with hire dates
UPDATE employees SET hire_date = DATE '2020-01-15' WHERE id = 1;
UPDATE employees SET hire_date = DATE '2020-03-20' WHERE id = 2;
UPDATE employees SET hire_date = DATE '2021-06-01' WHERE id = 3;

-- Add another table
CREATE TABLE departments (
    dept_id INTEGER,
    dept_name VARCHAR,
    budget DOUBLE,
    manager_id INTEGER
);

INSERT INTO departments VALUES
    (1, 'Engineering', 500000, 1),
    (2, 'Sales', 300000, 3);

-- Verify data
SELECT 'Employees after schema change:' as info;
SELECT * FROM employees ORDER BY id;

SELECT 'Departments:' as info;
SELECT * FROM departments ORDER BY dept_id;

-- ============================================
-- TEST 3: Create Child Branch (dev_branch)
-- ============================================
SELECT '=== TEST 3: Create Child Branch (dev_branch) ===' as test_section;

-- Create a development branch from main
CALL ducklake_create_branch('test_lake', 'dev_branch');

-- Verify branch was created
SELECT 'Branches after creating dev_branch:' as info;
SELECT branch_id, branch_name, parent_branch_id, head_snapshot_id
FROM __ducklake_metadata_test_lake."main".ducklake_branch
ORDER BY branch_id;

-- Switch to dev_branch
CALL ducklake_use_branch('test_lake', 'dev_branch');

-- Verify we can see inherited data
SELECT 'Dev branch inherited employee count' as test, COUNT(*) as result FROM employees;
-- Expected: 3 (inherited from main)

SELECT 'Dev branch inherited department count' as test, COUNT(*) as result FROM departments;
-- Expected: 2 (inherited from main)

-- ============================================
-- TEST 4: Independent Changes on Child Branch
-- ============================================
SELECT '=== TEST 4: Independent Changes on Child Branch ===' as test_section;

-- Add new employees only on dev_branch
INSERT INTO employees VALUES 
    (4, 'Diana', 'Engineering', 110000, DATE '2023-01-10'),
    (5, 'Eve', 'Marketing', 75000, DATE '2023-02-15');

-- Add a new column only on dev_branch
ALTER TABLE employees ADD COLUMN remote_worker BOOLEAN;

-- Set values
UPDATE employees SET remote_worker = false;
UPDATE employees SET remote_worker = true WHERE id = 4;

-- Create a new table only on dev_branch
CREATE TABLE projects (
    project_id INTEGER,
    project_name VARCHAR,
    lead_id INTEGER,
    start_date DATE,
    status VARCHAR
);

INSERT INTO projects VALUES
    (1, 'Project Alpha', 1, DATE '2023-01-01', 'active'),
    (2, 'Project Beta', 4, DATE '2023-06-01', 'planning');

-- Verify dev_branch state
SELECT 'Dev branch employee count after additions' as test, COUNT(*) as result FROM employees;
-- Expected: 5

SELECT 'Dev branch project count' as test, COUNT(*) as result FROM projects;
-- Expected: 2

SELECT 'Dev employees:' as info;
SELECT * FROM employees ORDER BY id;

-- ============================================
-- TEST 5: Verify Main Branch Isolation
-- ============================================
SELECT '=== TEST 5: Verify Main Branch Isolation ===' as test_section;

-- Switch back to main
CALL ducklake_use_branch('test_lake', 'main');

-- Main should NOT see dev_branch changes
SELECT 'Main branch employee count (should be 3, NOT 5)' as test, COUNT(*) as result FROM employees;

SELECT 'Main employees (should NOT have Diana, Eve, or remote_worker column):' as info;
SELECT * FROM employees ORDER BY id;

-- ============================================
-- TEST 6: Continue Main Branch Development
-- ============================================
SELECT '=== TEST 6: Continue Main Branch Development ===' as test_section;

-- Add more data to main (different from dev_branch)
INSERT INTO employees VALUES 
    (6, 'Frank', 'Sales', 82000, DATE '2023-03-01'),
    (7, 'Grace', 'Engineering', 105000, DATE '2023-04-15');

-- Add a different new column on main
ALTER TABLE employees ADD COLUMN performance_rating INTEGER;

UPDATE employees SET performance_rating = 5 WHERE id IN (1, 2);
UPDATE employees SET performance_rating = 4 WHERE id IN (3, 6, 7);

-- Verify main branch state
SELECT 'Main branch employee count after own additions' as test, COUNT(*) as result FROM employees;
-- Expected: 5 (original 3 + Frank + Grace)

SELECT 'Main employees with performance_rating:' as info;
SELECT id, name, department, performance_rating FROM employees ORDER BY id;

-- ============================================
-- TEST 7: Create Grandchild Branch
-- ============================================
SELECT '=== TEST 7: Create Grandchild Branch (feature_branch from dev_branch) ===' as test_section;

-- Switch to dev_branch first
CALL ducklake_use_branch('test_lake', 'dev_branch');

-- Create grandchild branch from dev_branch
CALL ducklake_create_branch('test_lake', 'feature_branch');

-- Verify branch hierarchy
SELECT 'All branches:' as info;
SELECT branch_id, branch_name, parent_branch_id, head_snapshot_id
FROM __ducklake_metadata_test_lake."main".ducklake_branch
ORDER BY branch_id;

-- Switch to feature_branch
CALL ducklake_use_branch('test_lake', 'feature_branch');

-- Verify inheritance from dev_branch
SELECT 'Feature branch employee count (inherited from dev)' as test, COUNT(*) as result FROM employees;
-- Expected: 5

SELECT 'Feature branch project count (inherited from dev)' as test, COUNT(*) as result FROM projects;
-- Expected: 2

-- ============================================
-- TEST 8: Grandchild Independent Lifecycle
-- ============================================
SELECT '=== TEST 8: Grandchild Independent Lifecycle ===' as test_section;

-- Add unique changes to feature_branch
INSERT INTO employees VALUES 
    (8, 'Henry', 'Research', 120000, DATE '2023-07-01', true);

-- Add a new table unique to feature_branch
CREATE TABLE experiments (
    exp_id INTEGER,
    exp_name VARCHAR,
    researcher_id INTEGER,
    hypothesis VARCHAR,
    status VARCHAR
);

INSERT INTO experiments VALUES
    (1, 'AI Enhancement', 8, 'Improve model accuracy by 20%', 'in_progress'),
    (2, 'Performance Opt', 4, 'Reduce latency by 50%', 'proposed');

-- Modify projects table
UPDATE projects SET status = 'completed' WHERE project_id = 1;
INSERT INTO projects VALUES (3, 'Project Gamma', 8, DATE '2023-08-01', 'active');

-- Verify feature_branch state
SELECT 'Feature branch employee count' as test, COUNT(*) as result FROM employees;
-- Expected: 6

SELECT 'Feature branch project count' as test, COUNT(*) as result FROM projects;
-- Expected: 3

SELECT 'Feature branch experiment count' as test, COUNT(*) as result FROM experiments;
-- Expected: 2

-- ============================================
-- TEST 9: Cross-Branch Isolation Verification
-- ============================================
SELECT '=== TEST 9: Cross-Branch Isolation Summary ===' as test_section;

-- Main branch
CALL ducklake_use_branch('test_lake', 'main');
SELECT 'MAIN: employee count' as branch_test, COUNT(*) as cnt FROM employees;

-- Dev branch
CALL ducklake_use_branch('test_lake', 'dev_branch');
SELECT 'DEV: employee count' as branch_test, COUNT(*) as cnt FROM employees;
SELECT 'DEV: project count' as branch_test, COUNT(*) as cnt FROM projects;

-- Feature branch
CALL ducklake_use_branch('test_lake', 'feature_branch');
SELECT 'FEATURE: employee count' as branch_test, COUNT(*) as cnt FROM employees;
SELECT 'FEATURE: project count' as branch_test, COUNT(*) as cnt FROM projects;
SELECT 'FEATURE: experiment count' as branch_test, COUNT(*) as cnt FROM experiments;

-- ============================================
-- TEST 10: Delete Operations Isolation
-- ============================================
SELECT '=== TEST 10: Delete Operations Isolation ===' as test_section;

-- Delete on feature_branch
CALL ducklake_use_branch('test_lake', 'feature_branch');
DELETE FROM employees WHERE id = 8;
SELECT 'FEATURE after delete (Henry removed)' as test, COUNT(*) as cnt FROM employees;
-- Expected: 5

-- Verify delete didn't affect dev_branch
CALL ducklake_use_branch('test_lake', 'dev_branch');
SELECT 'DEV still has 5 employees' as test, COUNT(*) as cnt FROM employees;

-- Delete on dev_branch
DELETE FROM employees WHERE id = 5;
SELECT 'DEV after delete (Eve removed)' as test, COUNT(*) as cnt FROM employees;
-- Expected: 4

-- Feature should still have Eve (inherited before dev's delete)
CALL ducklake_use_branch('test_lake', 'feature_branch');
SELECT 'FEATURE still has Eve' as test, COUNT(*) as cnt FROM employees WHERE name = 'Eve';

-- ============================================
-- TEST 11: Update Operations Isolation
-- ============================================
SELECT '=== TEST 11: Update Operations Isolation ===' as test_section;

-- Update on main
CALL ducklake_use_branch('test_lake', 'main');
UPDATE employees SET salary = salary * 1.1 WHERE department = 'Engineering';
SELECT 'MAIN Engineering salaries after 10% raise:' as info;
SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY id;

-- Dev should have original salaries
CALL ducklake_use_branch('test_lake', 'dev_branch');
SELECT 'DEV Engineering salaries (should be original):' as info;
SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY id;

-- ============================================
-- TEST 12: Metadata Integrity
-- ============================================
SELECT '=== TEST 12: Metadata Integrity ===' as test_section;

SELECT 'Snapshots with NULL branch_id (should be 0):' as check_name,
       COUNT(*) as count
FROM __ducklake_metadata_test_lake."main".ducklake_snapshot
WHERE branch_id IS NULL;

SELECT 'Tables with NULL branch_id (should be 0):' as check_name,
       COUNT(*) as count
FROM __ducklake_metadata_test_lake."main".ducklake_table
WHERE branch_id IS NULL;

SELECT 'Data files with NULL branch_id (should be 0):' as check_name,
       COUNT(*) as count
FROM __ducklake_metadata_test_lake."main".ducklake_data_file
WHERE branch_id IS NULL;

-- ============================================
-- TEST 13: Branch Summary
-- ============================================
SELECT '=== TEST 13: Final Branch Summary ===' as test_section;

SELECT 'All branches:' as info;
SELECT branch_id, branch_name, parent_branch_id, head_snapshot_id
FROM __ducklake_metadata_test_lake."main".ducklake_branch
ORDER BY branch_id;

SELECT 'Snapshots per branch:' as info;
SELECT branch_id, COUNT(*) as snapshot_count
FROM __ducklake_metadata_test_lake."main".ducklake_snapshot
GROUP BY branch_id
ORDER BY branch_id;

SELECT 'Active tables per branch:' as info;
SELECT branch_id, COUNT(*) as table_count
FROM __ducklake_metadata_test_lake."main".ducklake_table
WHERE end_snapshot IS NULL
GROUP BY branch_id
ORDER BY branch_id;

-- ============================================
-- TEST 14: Table Stats Verification
-- ============================================
SELECT '=== TEST 14: Table Stats by Branch ===' as test_section;

-- Show available columns
SELECT 'Table stats columns:' as info;
DESCRIBE __ducklake_metadata_test_lake."main".ducklake_table_stats;

SELECT 'Table stats data:' as info;
SELECT *
FROM __ducklake_metadata_test_lake."main".ducklake_table_stats
ORDER BY branch_id, table_id;

-- ============================================
SELECT '=== ALL COMPREHENSIVE TESTS COMPLETED ===' as test_section;
-- ============================================
