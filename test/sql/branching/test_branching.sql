-- ============================================================================
-- DuckLake Branching Test Suite - Manual SQL Tests
-- ============================================================================
-- IMPORTANT: Branches are READ-ONLY snapshots!
-- - All DML (INSERT/UPDATE/DELETE) goes to MAIN branch
-- - Use AT (BRANCH => 'name') in SELECT to read from snapshots
-- - Branches capture state at the time they were created
--
-- Run with: duckdb -unsigned < test_branching.sql
-- ============================================================================

.echo on

-- Load extension
LOAD 'build/release/extension/ducklake/ducklake.duckdb_extension';

-- Create fresh catalog
ATTACH 'ducklake:metadata=sqlite:branch_test.db data=./branch_data' AS test_catalog;
USE test_catalog;

-- ============================================================================
-- 1. VERIFY DEFAULT BRANCH EXISTS
-- ============================================================================

SELECT '=== TEST 1: Default branch ===' as test;
SELECT * FROM ducklake_branches('test_catalog');

-- ============================================================================
-- 2. CREATE TABLE AND DATA
-- ============================================================================

SELECT '=== TEST 2: Create table and data ===' as test;

CREATE TABLE users (
    id BIGINT,
    name VARCHAR,
    email VARCHAR
);

INSERT INTO users VALUES 
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com'),
    (3, 'Charlie', 'charlie@example.com');

SELECT * FROM users ORDER BY id;

-- ============================================================================
-- 3. CREATE BRANCH (snapshot at 3 users)
-- ============================================================================

SELECT '=== TEST 3: Create dev branch ===' as test;

SELECT * FROM ducklake_create_branch('test_catalog', 'dev');
SELECT * FROM ducklake_branches('test_catalog');
SELECT * FROM ducklake_branch_lineage('test_catalog', 'dev');

-- ============================================================================
-- 4. QUERY BOTH BRANCHES - they see same data initially
-- ============================================================================

SELECT '=== TEST 4: Both branches see same data ===' as test;

SELECT 'main branch:' as branch, COUNT(*) as cnt FROM users AT (BRANCH => 'main');
SELECT 'dev branch:' as branch, COUNT(*) as cnt FROM users AT (BRANCH => 'dev');

-- ============================================================================
-- 5. INSERT ON MAIN - dev won't see it (forked before)
-- ============================================================================

SELECT '=== TEST 5: Insert on main ===' as test;

INSERT INTO users VALUES (4, 'Dave', 'dave@example.com');

SELECT 'After INSERT - main:' as info, COUNT(*) as cnt FROM users AT (BRANCH => 'main');
SELECT 'After INSERT - dev:' as info, COUNT(*) as cnt FROM users AT (BRANCH => 'dev');

-- ============================================================================
-- 6. UPDATE ON MAIN - dev won't see it
-- ============================================================================

SELECT '=== TEST 6: Update on main ===' as test;

UPDATE users SET email = 'bob.updated@example.com' WHERE id = 2;

SELECT 'Main branch - Bob email:' as info, email FROM users AT (BRANCH => 'main') WHERE id = 2;
SELECT 'Dev branch - Bob email:' as info, email FROM users AT (BRANCH => 'dev') WHERE id = 2;

-- ============================================================================
-- 7. DELETE ON MAIN - dev won't see it
-- ============================================================================

SELECT '=== TEST 7: Delete on main ===' as test;

DELETE FROM users WHERE id = 1;

SELECT 'After DELETE - main:' as info;
SELECT * FROM users AT (BRANCH => 'main') ORDER BY id;

SELECT 'After DELETE - dev (still has Alice):' as info;
SELECT * FROM users AT (BRANCH => 'dev') ORDER BY id;

-- ============================================================================
-- 8. CREATE NESTED BRANCH
-- ============================================================================

SELECT '=== TEST 8: Nested branches ===' as test;

-- Create feature branch from dev
SELECT * FROM ducklake_create_branch('test_catalog', 'feature', 'dev');

-- Verify lineage
SELECT * FROM ducklake_branches('test_catalog');
SELECT * FROM ducklake_branch_lineage('test_catalog', 'feature');

-- Feature inherits from dev (sees original 3 users)
SELECT 'Feature branch (from dev):' as info;
SELECT * FROM users AT (BRANCH => 'feature') ORDER BY id;

-- ============================================================================
-- 9. VERSION SYNTAX
-- ============================================================================

SELECT '=== TEST 9: Version syntax ===' as test;

-- Query at version 1 (after CREATE TABLE, before data)
SELECT 'At version 1 (empty):' as info, COUNT(*) as cnt FROM users AT (VERSION => 1);

-- Query at version 2 (after initial INSERT of 3 users)
SELECT 'At version 2 (3 users):' as info, COUNT(*) as cnt FROM users AT (VERSION => 2);

-- ============================================================================
-- 10. JOINS ACROSS BRANCHES
-- ============================================================================

SELECT '=== TEST 10: Cross-branch joins ===' as test;

CREATE TABLE orders (
    id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10,2)
);

INSERT INTO orders VALUES (1, 1, 100.00), (2, 2, 200.00);

-- Create snapshot before adding Dave's order
SELECT * FROM ducklake_create_branch('test_catalog', 'orders_v1');

-- Add order for Dave
INSERT INTO orders VALUES (3, 4, 300.00);

-- Join on main - sees Dave's order
SELECT 'Main branch join:' as info;
SELECT o.id, u.name, o.amount 
FROM orders AT (BRANCH => 'main') o 
JOIN users AT (BRANCH => 'main') u ON o.user_id = u.id 
ORDER BY o.id;

-- Join on dev - Dave doesn't exist in users@dev, order 3 not in orders@dev
SELECT 'Dev branch join:' as info;
SELECT o.id, u.name, o.amount 
FROM orders AT (BRANCH => 'dev') o 
JOIN users AT (BRANCH => 'dev') u ON o.user_id = u.id 
ORDER BY o.id;

-- ============================================================================
-- 11. BRANCH DELETION
-- ============================================================================

SELECT '=== TEST 11: Branch deletion ===' as test;

-- Delete feature first (leaf)
SELECT * FROM ducklake_delete_branch('test_catalog', 'feature');

-- Now can delete dev
SELECT * FROM ducklake_delete_branch('test_catalog', 'dev');

SELECT * FROM ducklake_branches('test_catalog');

-- ============================================================================
-- CLEANUP
-- ============================================================================

SELECT '=== CLEANUP ===' as test;

DROP TABLE orders;
DROP TABLE users;
DETACH test_catalog;

SELECT '=== ALL TESTS COMPLETE ===' as status;
