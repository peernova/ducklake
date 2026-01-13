-- Test: Compact files that have deletes, then query from different branches
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:dbname=compact_delete_branch host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/compact_delete_branch');
USE t;

-- Create table with data
CREATE TABLE orders(id INT, customer VARCHAR, amount DECIMAL);

-- Insert data in multiple batches (creates multiple files)
INSERT INTO orders SELECT i, 'cust_' || (i % 10), i * 10.0 FROM range(1, 101) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 10), i * 10.0 FROM range(101, 201) r(i);
INSERT INTO orders SELECT i, 'cust_' || (i % 10), i * 10.0 FROM range(201, 301) r(i);

SELECT 'Main after inserts: ' || COUNT(*) as msg FROM orders;

-- Delete some rows on main (creates delete files)
DELETE FROM orders WHERE id <= 20;
SELECT 'Main after delete (id<=20): ' || COUNT(*) as msg FROM orders;

-- Create branch_a
CALL ducklake_create_branch('t', 'branch_a');
CALL ducklake_use_branch('t', 'branch_a');

-- Delete different rows on branch_a
DELETE FROM orders WHERE id > 280;
SELECT 'branch_a after delete (id>280): ' || COUNT(*) as msg FROM orders;

-- Now compact on branch_a - this should merge files AND handle delete files
SELECT 'branch_a: Running compaction with delete files present...' as msg;
CALL ducklake_rewrite_data_files('t', 'orders', delete_threshold := 0.01);

SELECT 'branch_a after compaction: ' || COUNT(*) as msg FROM orders;

-- Check what main sees
CALL ducklake_use_branch('t', 'main');
SELECT 'Main after branch_a compaction: ' || COUNT(*) as msg FROM orders;

-- Verify specific rows
SELECT 'Main - rows with id <= 20: ' || COUNT(*) as msg FROM orders WHERE id <= 20;
SELECT 'Main - rows with id > 280: ' || COUNT(*) as msg FROM orders WHERE id > 280;

CALL ducklake_use_branch('t', 'branch_a');
SELECT 'branch_a - rows with id <= 20: ' || COUNT(*) as msg FROM orders WHERE id <= 20;
SELECT 'branch_a - rows with id > 280: ' || COUNT(*) as msg FROM orders WHERE id > 280;
