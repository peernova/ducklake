-- ============================================================================
-- DuckLake Branching Interactive Test Suite
-- ============================================================================
-- Branches are READ-ONLY snapshots. All DML goes to main branch.
-- Use AT (BRANCH => 'name') to SELECT from branch snapshots.
--
-- Run with: 
--   cd /path/to/ducklake
--   make GEN=ninja release
--   ./build/release/duckdb -unsigned < test/sql/branching/test_interactive_branching.sql
-- ============================================================================

.echo on
.timer on

-- ============================================================================
-- SETUP: Load extension and create catalog
-- ============================================================================

LOAD 'build/release/extension/ducklake/ducklake.duckdb_extension';

-- Create fresh catalog
ATTACH 'ducklake:interactive_test.ducklake' AS dl;
USE dl;

SELECT '=== DuckLake Branching Test ===' as info;

-- ============================================================================
-- 1. VERIFY INITIAL STATE
-- ============================================================================

SELECT '=== 1. Initial Branch State ===' as test;
SELECT * FROM ducklake_branches('dl');

-- ============================================================================
-- 2. CREATE SAMPLE DATA
-- ============================================================================

SELECT '=== 2. Create Sample Data ===' as test;

CREATE TABLE employees (
    id BIGINT,
    name VARCHAR,
    department VARCHAR,
    salary DECIMAL(10,2)
);

INSERT INTO employees VALUES 
    (1, 'Alice', 'Engineering', 100000),
    (2, 'Bob', 'Engineering', 90000),
    (3, 'Charlie', 'Sales', 80000),
    (4, 'Diana', 'Sales', 85000),
    (5, 'Eve', 'Marketing', 75000);

SELECT 'Main branch data:' as info;
SELECT * FROM employees ORDER BY id;

-- ============================================================================
-- 3. CREATE DEVELOPMENT BRANCH (snapshot at 5 employees)
-- ============================================================================

SELECT '=== 3. Create Development Branch ===' as test;

SELECT * FROM ducklake_create_branch('dl', 'develop');
SELECT * FROM ducklake_branches('dl');

-- ============================================================================
-- 4. MAKE CHANGES ON MAIN (develop won't see them)
-- ============================================================================

SELECT '=== 4. Make Changes on Main ===' as test;

-- Add new employee on main
INSERT INTO employees VALUES (6, 'Frank', 'Engineering', 95000);

-- Give Engineering a raise on main
UPDATE employees SET salary = salary * 1.10 WHERE department = 'Engineering';

-- Verify changes on main
SELECT 'Main branch data after changes:' as info;
SELECT * FROM employees AT (BRANCH => 'main') ORDER BY id;

-- Verify develop is unchanged (forked before changes)
SELECT 'Develop branch data (should be original 5 employees):' as info;
SELECT * FROM employees AT (BRANCH => 'develop') ORDER BY id;

-- ============================================================================
-- 5. CREATE ANOTHER BRANCH (after changes)
-- ============================================================================

SELECT '=== 5. Create Branch After Changes ===' as test;

SELECT * FROM ducklake_create_branch('dl', 'release_v1');
SELECT * FROM ducklake_branches('dl');

-- release_v1 sees current main state (6 employees with raises)
SELECT 'release_v1 sees current main state:' as info;
SELECT * FROM employees AT (BRANCH => 'release_v1') ORDER BY id;

-- ============================================================================
-- 6. CROSS-BRANCH COMPARISON
-- ============================================================================

SELECT '=== 6. Cross-Branch Comparison ===' as test;

-- Compare counts
SELECT 
    'main' as branch, COUNT(*) as emp_count, ROUND(AVG(salary), 2) as avg_salary
FROM employees AT (BRANCH => 'main')
UNION ALL
SELECT 
    'develop' as branch, COUNT(*) as emp_count, ROUND(AVG(salary), 2) as avg_salary
FROM employees AT (BRANCH => 'develop')
UNION ALL
SELECT 
    'release_v1' as branch, COUNT(*) as emp_count, ROUND(AVG(salary), 2) as avg_salary
FROM employees AT (BRANCH => 'release_v1');

-- Compare salaries for Alice across branches
SELECT 
    'main' as branch, salary FROM employees AT (BRANCH => 'main') WHERE name = 'Alice'
UNION ALL
SELECT 
    'develop' as branch, salary FROM employees AT (BRANCH => 'develop') WHERE name = 'Alice'
UNION ALL
SELECT
    'release_v1' as branch, salary FROM employees AT (BRANCH => 'release_v1') WHERE name = 'Alice';

-- ============================================================================
-- 7. CREATE NESTED BRANCH
-- ============================================================================

SELECT '=== 7. Create Nested Branch from Develop ===' as test;

SELECT * FROM ducklake_create_branch('dl', 'feature_x', 'develop');
SELECT * FROM ducklake_branch_lineage('dl', 'feature_x');

-- feature_x sees what develop sees (original 5 employees)
SELECT 'feature_x inherits from develop:' as info;
SELECT COUNT(*) as count FROM employees AT (BRANCH => 'feature_x');

-- ============================================================================
-- 8. BRANCH HIERARCHY VISUALIZATION
-- ============================================================================

SELECT '=== 8. Branch Hierarchy ===' as test;

SELECT 
    b.branch_id,
    b.branch_name,
    p.branch_name as parent,
    b.fork_snapshot_id,
    b.head_snapshot_id
FROM ducklake_branches('dl') b
LEFT JOIN ducklake_branches('dl') p ON b.parent_branch_id = p.branch_id
ORDER BY b.branch_id;

-- ============================================================================
-- 9. MORE CHANGES ON MAIN
-- ============================================================================

SELECT '=== 9. More Changes on Main ===' as test;

-- Add another employee
INSERT INTO employees VALUES (7, 'Grace', 'Research', 120000);

-- Delete an employee
DELETE FROM employees WHERE id = 3;

SELECT 'Final main state:' as info;
SELECT * FROM employees AT (BRANCH => 'main') ORDER BY id;

-- All branches still have their snapshot data
SELECT 'All branches comparison:' as info;
SELECT 
    (SELECT COUNT(*) FROM employees AT (BRANCH => 'main')) as main_cnt,
    (SELECT COUNT(*) FROM employees AT (BRANCH => 'develop')) as develop_cnt,
    (SELECT COUNT(*) FROM employees AT (BRANCH => 'release_v1')) as release_cnt,
    (SELECT COUNT(*) FROM employees AT (BRANCH => 'feature_x')) as feature_cnt;

-- ============================================================================
-- 10. DELETE BRANCHES
-- ============================================================================

SELECT '=== 10. Delete Branches ===' as test;

-- Delete leaf branches first
SELECT * FROM ducklake_delete_branch('dl', 'feature_x');
SELECT * FROM ducklake_delete_branch('dl', 'release_v1');

-- Now can delete develop (no children)
SELECT * FROM ducklake_delete_branch('dl', 'develop');

-- Show remaining branches
SELECT * FROM ducklake_branches('dl');

-- ============================================================================
-- 11. DEPARTMENT SUMMARY
-- ============================================================================

SELECT '=== 11. Department Summary ===' as test;

SELECT department, COUNT(*) as cnt, ROUND(SUM(salary), 2) as total_sal 
FROM employees AT (BRANCH => 'main') 
GROUP BY department 
ORDER BY department;

-- ============================================================================
-- CLEANUP
-- ============================================================================

SELECT '=== CLEANUP ===' as test;

DROP TABLE employees;
DETACH dl;

SELECT '=== ALL TESTS COMPLETE ===' as status;
