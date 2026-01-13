-- Comprehensive test: Deep branch tree with compaction and time travel
-- B5 has depth 6, diameter 3
-- Complex sequence: cuts before/after compaction, time travel validation

LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=deep_tree_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/deep_tree_test');
USE t;

-- ============================================================================
-- PHASE 1: Setup main with initial data
-- ============================================================================
SELECT '=== PHASE 1: Setup ===' as phase;

CREATE TABLE orders(id INT, customer VARCHAR, amount DECIMAL, status VARCHAR);

-- Insert 1000 rows in batches (creates multiple files for compaction)
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, 'pending' FROM range(1, 201) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, 'pending' FROM range(201, 401) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, 'pending' FROM range(401, 601) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, 'pending' FROM range(601, 801) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 100), i * 10.0, 'pending' FROM range(801, 1001) r(i);

SELECT 'main initial: ' || COUNT(*) as msg FROM orders;

-- Create level 1 branches
CALL ducklake_create_branch('t', 'b1');
CALL ducklake_create_branch('t', 'b2');
CALL ducklake_create_branch('t', 'b3');
CALL ducklake_create_branch('t', 'b4');
CALL ducklake_create_branch('t', 'b5');

-- ============================================================================
-- PHASE 2: B1-B4 simple operations (for comparison)
-- ============================================================================
SELECT '=== PHASE 2: B1-B4 operations ===' as phase;

CALL ducklake_use_branch('t', 'b1');
DELETE FROM orders WHERE id <= 50;
SELECT 'b1 after delete (id<=50): ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b2');
DELETE FROM orders WHERE id > 950;
SELECT 'b2 after delete (id>950): ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b3');
UPDATE orders SET amount = amount * 2 WHERE id BETWEEN 200 AND 250;
SELECT 'b3 after update: ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b4');
INSERT INTO orders SELECT i, 'b4_new', i * 5.0, 'new' FROM range(1001, 1051) r(i);
SELECT 'b4 after insert: ' || COUNT(*) as msg FROM orders;

-- ============================================================================
-- PHASE 3: B5 Deep Tree - Level 2 (DELETES)
-- ============================================================================
SELECT '=== PHASE 3: B5 Level 2 - Deletes ===' as phase;

CALL ducklake_use_branch('t', 'b5');
CALL ducklake_create_branch('t', 'b5a');
CALL ducklake_create_branch('t', 'b5b');
CALL ducklake_create_branch('t', 'b5c');

CALL ducklake_use_branch('t', 'b5a');
DELETE FROM orders WHERE id BETWEEN 1 AND 30;
SELECT 'b5a after delete (1-30): ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b5b');
DELETE FROM orders WHERE id BETWEEN 31 AND 60;
SELECT 'b5b after delete (31-60): ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b5c');
DELETE FROM orders WHERE id BETWEEN 61 AND 90;
SELECT 'b5c after delete (61-90): ' || COUNT(*) as msg FROM orders;

-- ============================================================================
-- PHASE 4: B5 Level 3 (UPDATES) - branch from b5a
-- ============================================================================
SELECT '=== PHASE 4: B5 Level 3 - Updates ===' as phase;

CALL ducklake_use_branch('t', 'b5a');
CALL ducklake_create_branch('t', 'b5a1');
CALL ducklake_create_branch('t', 'b5a2');
CALL ducklake_create_branch('t', 'b5a3');

CALL ducklake_use_branch('t', 'b5a1');
UPDATE orders SET status = 'updated_L3' WHERE id BETWEEN 100 AND 150;
SELECT 'b5a1 after update: ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b5a2');
UPDATE orders SET amount = amount * 3 WHERE id BETWEEN 151 AND 200;
SELECT 'b5a2 after update: ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b5a3');
UPDATE orders SET customer = 'updated_cust' WHERE id BETWEEN 201 AND 250;
SELECT 'b5a3 after update: ' || COUNT(*) as msg FROM orders;

-- ============================================================================
-- PHASE 5: B5 Level 4 - deletes+inserts+updates, COMPACT, then delete
-- ============================================================================
SELECT '=== PHASE 5: B5 Level 4 - Operations + Compact ===' as phase;

CALL ducklake_use_branch('t', 'b5a1');
CALL ducklake_create_branch('t', 'b5a1x');
CALL ducklake_create_branch('t', 'b5a1y');
CALL ducklake_create_branch('t', 'b5a1z');

CALL ducklake_use_branch('t', 'b5a1x');
-- Mixed operations
DELETE FROM orders WHERE id BETWEEN 300 AND 320;
INSERT INTO orders SELECT i, 'L4_insert', i * 7.0, 'L4_new' FROM range(1051, 1081) r(i);
UPDATE orders SET status = 'L4_updated' WHERE id BETWEEN 400 AND 420;
SELECT 'b5a1x after ops: ' || COUNT(*) as msg FROM orders;

-- Record snapshot BEFORE compact for time travel
SELECT 'b5a1x snapshot before compact: ' || MAX(snapshot_id) as msg
FROM ducklake_snapshots('t') WHERE branch_name = 'b5a1x';

-- ============================================================================
-- PHASE 6: Cut Level 5 BEFORE Level 4 compaction
-- ============================================================================
SELECT '=== PHASE 6: Cut Level 5 before L4 compact ===' as phase;

CALL ducklake_use_branch('t', 'b5a1x');
CALL ducklake_create_branch('t', 'b5a1x_L5_early');

SELECT 'b5a1x_L5_early created before compact: ' || COUNT(*) as msg FROM orders;
CALL ducklake_use_branch('t', 'b5a1x_L5_early');
SELECT 'b5a1x_L5_early sees: ' || COUNT(*) as msg FROM orders;

-- ============================================================================
-- PHASE 7: Level 4 COMPACTS
-- ============================================================================
SELECT '=== PHASE 7: Level 4 Compacts ===' as phase;

CALL ducklake_use_branch('t', 'b5a1x');
SELECT 'b5a1x before compact: ' || COUNT(*) as msg FROM orders;
CALL ducklake_rewrite_data_files('t', 'orders', delete_threshold := 0.01);
SELECT 'b5a1x after compact: ' || COUNT(*) as msg FROM orders;

-- Record snapshot AFTER compact
SELECT 'b5a1x snapshot after compact: ' || MAX(snapshot_id) as msg
FROM ducklake_snapshots('t') WHERE branch_name = 'b5a1x';

-- More operations after compact
DELETE FROM orders WHERE id BETWEEN 500 AND 510;
SELECT 'b5a1x after post-compact delete: ' || COUNT(*) as msg FROM orders;

-- ============================================================================
-- PHASE 8: Level 5 (early cut) does its own operations + compact
-- ============================================================================
SELECT '=== PHASE 8: Level 5 early operations + compact ===' as phase;

CALL ducklake_use_branch('t', 'b5a1x_L5_early');
-- Different operations than parent
DELETE FROM orders WHERE id BETWEEN 600 AND 630;
INSERT INTO orders SELECT i, 'L5_early_insert', i * 8.0, 'L5_new' FROM range(1081, 1111) r(i);
SELECT 'L5_early after ops: ' || COUNT(*) as msg FROM orders;

-- Record snapshot before L5 compact
SELECT 'L5_early snapshot before compact: ' || MAX(snapshot_id) as msg
FROM ducklake_snapshots('t') WHERE branch_name = 'b5a1x_L5_early';

CALL ducklake_rewrite_data_files('t', 'orders', delete_threshold := 0.01);
SELECT 'L5_early after compact: ' || COUNT(*) as msg FROM orders;

-- Record snapshot after L5 compact
SELECT 'L5_early snapshot after compact: ' || MAX(snapshot_id) as msg
FROM ducklake_snapshots('t') WHERE branch_name = 'b5a1x_L5_early';

-- ============================================================================
-- PHASE 9: Level 4 does more ops, compacts again, cuts Level 6
-- ============================================================================
SELECT '=== PHASE 9: Level 4 second compact + cut Level 6 ===' as phase;

CALL ducklake_use_branch('t', 'b5a1x');
INSERT INTO orders SELECT i, 'L4_second_insert', i * 9.0, 'L4_v2' FROM range(1111, 1141) r(i);
DELETE FROM orders WHERE id BETWEEN 700 AND 710;
SELECT 'b5a1x after second ops: ' || COUNT(*) as msg FROM orders;

CALL ducklake_rewrite_data_files('t', 'orders', delete_threshold := 0.01);
SELECT 'b5a1x after second compact: ' || COUNT(*) as msg FROM orders;

-- Now cut Level 6 from Level 5 (after L5 compact)
CALL ducklake_use_branch('t', 'b5a1x_L5_early');
CALL ducklake_create_branch('t', 'b5a1x_L6');

SELECT 'L6 created from L5 post-compact: ' || COUNT(*) as msg FROM orders;

-- ============================================================================
-- PHASE 10: Level 6 operations
-- ============================================================================
SELECT '=== PHASE 10: Level 6 operations ===' as phase;

CALL ducklake_use_branch('t', 'b5a1x_L6');
DELETE FROM orders WHERE id BETWEEN 800 AND 820;
INSERT INTO orders SELECT i, 'L6_insert', i * 10.0, 'L6_new' FROM range(1141, 1171) r(i);
UPDATE orders SET status = 'L6_final' WHERE id BETWEEN 900 AND 920;
SELECT 'L6 after all ops: ' || COUNT(*) as msg FROM orders;

-- ============================================================================
-- PHASE 11: Level 5 more operations after L6 cut
-- ============================================================================
SELECT '=== PHASE 11: Level 5 post-L6-cut operations ===' as phase;

CALL ducklake_use_branch('t', 'b5a1x_L5_early');
DELETE FROM orders WHERE id BETWEEN 850 AND 860;
INSERT INTO orders SELECT i, 'L5_late_insert', i * 11.0, 'L5_v2' FROM range(1171, 1191) r(i);
SELECT 'L5_early after post-L6 ops: ' || COUNT(*) as msg FROM orders;

-- ============================================================================
-- PHASE 12: Record all snapshots for time travel
-- ============================================================================
SELECT '=== PHASE 12: Snapshot inventory ===' as phase;

SELECT branch_name, snapshot_id, changes_made
FROM ducklake_snapshots('t')
WHERE branch_name IN ('b5a1x', 'b5a1x_L5_early', 'b5a1x_L6')
ORDER BY branch_name, snapshot_id;

-- ============================================================================
-- PHASE 13: Time Travel Validation - Level 4 (b5a1x)
-- ============================================================================
SELECT '=== PHASE 13: Time Travel - Level 4 ===' as phase;

CALL ducklake_use_branch('t', 'b5a1x');

-- Get current count
SELECT 'L4 current: ' || COUNT(*) as msg FROM orders;

-- Time travel to various points (we'll use AT SNAPSHOT)
-- Note: Need to find actual snapshot IDs from Phase 12 output

-- ============================================================================
-- PHASE 14: Time Travel Validation - Level 5
-- ============================================================================
SELECT '=== PHASE 14: Time Travel - Level 5 ===' as phase;

CALL ducklake_use_branch('t', 'b5a1x_L5_early');
SELECT 'L5 current: ' || COUNT(*) as msg FROM orders;

-- ============================================================================
-- PHASE 15: Time Travel Validation - Level 6
-- ============================================================================
SELECT '=== PHASE 15: Time Travel - Level 6 ===' as phase;

CALL ducklake_use_branch('t', 'b5a1x_L6');
SELECT 'L6 current: ' || COUNT(*) as msg FROM orders;

-- ============================================================================
-- PHASE 16: Final Validation - All Branches
-- ============================================================================
SELECT '=== PHASE 16: Final Validation ===' as phase;

CALL ducklake_use_branch('t', 'main');
SELECT 'main: ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b1');
SELECT 'b1: ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b2');
SELECT 'b2: ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b3');
SELECT 'b3: ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b4');
SELECT 'b4: ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b5');
SELECT 'b5: ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b5a');
SELECT 'b5a: ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b5a1');
SELECT 'b5a1: ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b5a1x');
SELECT 'b5a1x (L4): ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b5a1x_L5_early');
SELECT 'b5a1x_L5_early: ' || COUNT(*) as msg FROM orders;

CALL ducklake_use_branch('t', 'b5a1x_L6');
SELECT 'b5a1x_L6: ' || COUNT(*) as msg FROM orders;

SELECT '=== TEST COMPLETE ===' as phase;
