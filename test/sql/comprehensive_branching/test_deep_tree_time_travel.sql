-- Time Travel validation for deep tree test
-- Run AFTER test_deep_tree_compaction.sql

LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=deep_tree_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/deep_tree_test');
USE t;

-- ============================================================================
-- Show branch structure and key snapshots
-- ============================================================================
SELECT '=== Branch Structure ===' as section;
SELECT branch_id, branch_name, parent_branch_id, fork_snapshot_id, head_snapshot_id
FROM ducklake_branches('t')
ORDER BY branch_id;

-- ============================================================================
-- Current counts on all key branches
-- ============================================================================
SELECT '=== Current Counts ===' as section;

CALL ducklake_use_branch('t', 'main');
SELECT 'main current: ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b5a1x');
SELECT 'b5a1x (L4) current: ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b5a1x_L5_early');
SELECT 'L5_early current: ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b5a1x_L6');
SELECT 'L6 current: ' || COUNT(*) as msg FROM orders;

-- ============================================================================
-- TIME TRAVEL: b5a1x (Level 4)
-- fork_snapshot=8, head=16
-- Key points: before first compact, after first compact, after second compact
-- ============================================================================
SELECT '=== Time Travel: b5a1x (L4) ===' as section;

CALL ducklake_use_branch('t', 'b5a1x');

-- At fork point (snapshot 8) - should see inherited state
SELECT 'b5a1x AT SNAPSHOT 8 (fork): ' || COUNT(*) as msg FROM orders AT SNAPSHOT 8;

-- At snapshot 9 (after first ops, before compact)
SELECT 'b5a1x AT SNAPSHOT 9: ' || COUNT(*) as msg FROM orders AT SNAPSHOT 9;

-- At snapshot 11 (after first compact)
SELECT 'b5a1x AT SNAPSHOT 11: ' || COUNT(*) as msg FROM orders AT SNAPSHOT 11;

-- At snapshot 12 (after post-compact delete)
SELECT 'b5a1x AT SNAPSHOT 12: ' || COUNT(*) as msg FROM orders AT SNAPSHOT 12;

-- At snapshot 15 (after second ops)
SELECT 'b5a1x AT SNAPSHOT 15: ' || COUNT(*) as msg FROM orders AT SNAPSHOT 15;

-- At snapshot 16 (current/head after second compact)
SELECT 'b5a1x AT SNAPSHOT 16 (head): ' || COUNT(*) as msg FROM orders AT SNAPSHOT 16;

-- ============================================================================
-- TIME TRAVEL: b5a1x_L5_early (Level 5)
-- fork_snapshot=11, head=16
-- Cut BEFORE L4's first compact, so doesn't see compacted files
-- ============================================================================
SELECT '=== Time Travel: L5_early ===' as section;

CALL ducklake_use_branch('t', 'b5a1x_L5_early');

-- At fork point (snapshot 11) - same state as b5a1x at 11
SELECT 'L5_early AT SNAPSHOT 11 (fork): ' || COUNT(*) as msg FROM orders AT SNAPSHOT 11;

-- At snapshot 13 (after L5's own ops)
SELECT 'L5_early AT SNAPSHOT 13: ' || COUNT(*) as msg FROM orders AT SNAPSHOT 13;

-- At snapshot 14 (after L5 compact)
SELECT 'L5_early AT SNAPSHOT 14: ' || COUNT(*) as msg FROM orders AT SNAPSHOT 14;

-- At snapshot 16 (current/head)
SELECT 'L5_early AT SNAPSHOT 16 (head): ' || COUNT(*) as msg FROM orders AT SNAPSHOT 16;

-- ============================================================================
-- TIME TRAVEL: b5a1x_L6 (Level 6)
-- fork_snapshot=14, head=17
-- Cut AFTER L5's compact
-- ============================================================================
SELECT '=== Time Travel: L6 ===' as section;

CALL ducklake_use_branch('t', 'b5a1x_L6');

-- At fork point (snapshot 14) - same as L5 at 14
SELECT 'L6 AT SNAPSHOT 14 (fork): ' || COUNT(*) as msg FROM orders AT SNAPSHOT 14;

-- At snapshot 17 (current/head after L6 ops)
SELECT 'L6 AT SNAPSHOT 17 (head): ' || COUNT(*) as msg FROM orders AT SNAPSHOT 17;

-- ============================================================================
-- Cross-branch isolation check
-- After all operations, verify branches don't see each other's changes
-- ============================================================================
SELECT '=== Cross-Branch Isolation ===' as section;

-- L4 should NOT see L5's inserts (L5_early_insert, L5_late_insert)
CALL ducklake_use_branch('t', 'b5a1x');
SELECT 'b5a1x sees L5 inserts: ' || COUNT(*) as msg FROM orders WHERE customer LIKE 'L5%';

-- L5 should NOT see L4's second batch of inserts (L4_second_insert)
CALL ducklake_use_branch('t', 'b5a1x_L5_early');
SELECT 'L5 sees L4_second_insert: ' || COUNT(*) as msg FROM orders WHERE customer = 'L4_second_insert';

-- L6 should see L5's inserts but NOT L5's post-L6-cut inserts
CALL ducklake_use_branch('t', 'b5a1x_L6');
SELECT 'L6 sees L5_early_insert: ' || COUNT(*) as msg FROM orders WHERE customer = 'L5_early_insert';
SELECT 'L6 sees L5_late_insert: ' || COUNT(*) as msg FROM orders WHERE customer = 'L5_late_insert';

-- Main should not see any branch inserts
CALL ducklake_use_branch('t', 'main');
SELECT 'main sees L4 inserts: ' || COUNT(*) as msg FROM orders WHERE customer LIKE 'L4%';
SELECT 'main sees L5 inserts: ' || COUNT(*) as msg FROM orders WHERE customer LIKE 'L5%';
SELECT 'main sees L6 inserts: ' || COUNT(*) as msg FROM orders WHERE customer LIKE 'L6%';

SELECT '=== TIME TRAVEL TEST COMPLETE ===' as section;
