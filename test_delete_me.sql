LOAD 'build/release/extension/ducklake/ducklake.duckdb_extension';

-- Clean start
ATTACH 'ducklake:test_debug.ducklake' AS test_lake;
USE test_lake;

-- Create departments on main
CREATE TABLE test_lake.main.departments (id INT, name VARCHAR, budget DECIMAL(15,2));
INSERT INTO test_lake.main.departments VALUES 
    (1, 'Engineering', 1000000.00),
    (2, 'Sales', 500000.00),
    (3, 'Marketing', 300000.00);

-- Create budget_branch
CALL ducklake_create_branch('test_lake', 'budget_branch');
CALL ducklake_use_branch('test_lake', 'budget_branch');

-- Insert and Update
INSERT INTO test_lake.main.departments VALUES (4, 'HR', 200000.00);
UPDATE test_lake.main.departments SET budget = 1500000.00 WHERE name = 'Engineering';

-- Check current snapshot
SELECT * FROM ducklake_current_branch('test_lake');

-- Query all data
SELECT 'All departments:' as info;
SELECT * FROM test_lake.main.departments ORDER BY id;

-- Query with WHERE
SELECT 'Engineering row:' as info;
SELECT * FROM test_lake.main.departments WHERE name = 'Engineering';


-- Check what's in the actual parquet files
SELECT branch_id, data_file_id, path, record_count 
FROM "__ducklake_metadata_test_lake".main.ducklake_data_file 
WHERE table_id = 1
ORDER BY branch_id, data_file_id;

-- Check delete files
SELECT * FROM "__ducklake_metadata_test_lake".main.ducklake_delete_file WHERE table_id = 1;


-- List all tables in the metadata catalog
SELECT table_name FROM information_schema.tables 
WHERE table_catalog = '__ducklake_metadata_test_lake'
ORDER BY table_name;

-- Check the inlined data tables registry
SELECT * FROM "__ducklake_metadata_test_lake".main.ducklake_inlined_data_tables;

-- Check the snapshot changes to see what operations were recorded
SELECT * FROM "__ducklake_metadata_test_lake".main.ducklake_snapshot_changes 
ORDER BY branch_id, snapshot_id;

-- Check all snapshots
SELECT * FROM "__ducklake_metadata_test_lake".main.ducklake_snapshot 
ORDER BY branch_id, snapshot_id;

-- Check the table stats
SELECT * FROM "__ducklake_metadata_test_lake".main.ducklake_table_stats;

-- Simulate GetFilesForTable for budget_branch (branch_id=1) at snapshot_id=3
-- for departments table (table_id=1)
SELECT data.branch_id, data.data_file_id, data.path, data.record_count
FROM "__ducklake_metadata_test_lake".main.ducklake_data_file data
JOIN "__ducklake_metadata_test_lake".main.ducklake_branch_lineage bl 
  ON data.branch_id = bl.ancestor_branch_id
WHERE bl.branch_id = 1
  AND data.table_id = 1
  AND CASE WHEN data.branch_id = 1 THEN 3 ELSE bl.max_visible_snapshot END >= data.begin_snapshot
  AND (data.end_snapshot IS NULL OR CASE WHEN data.branch_id = 1 THEN 3 ELSE bl.max_visible_snapshot END < data.end_snapshot)
ORDER BY data.branch_id, data.data_file_id;

-- Also check the lineage for branch 1
SELECT * FROM "__ducklake_metadata_test_lake".main.ducklake_branch_lineage WHERE branch_id = 1;



-- Read the actual parquet file for branch 0 (main's departments)
SELECT * FROM parquet_scan('test_debug.ducklake.files/main/departments/ducklake-019b5c14-d92c-746f-b59d-930245ebd048.parquet');

-- Read the actual parquet file for branch 1 (HR insert)
SELECT * FROM parquet_scan('test_debug.ducklake.files/main/departments/ducklake-019b5c14-d93c-72ee-8dfc-9be8b4da984a.parquet');




-- Check the column definitions for departments
SELECT * FROM "__ducklake_metadata_test_lake".main.ducklake_column WHERE table_id = 1 ORDER BY column_order;

-- Try selecting specific columns to see what we get
SELECT id, name, budget FROM test_lake.main.departments ORDER BY id;



-- Check the data file details including row_id_start
SELECT branch_id, data_file_id, table_id, begin_snapshot, row_id_start, record_count, path
FROM "__ducklake_metadata_test_lake".main.ducklake_data_file 
WHERE table_id = 1
ORDER BY branch_id, data_file_id;


-- What table_id does departments have?
SELECT * FROM "__ducklake_metadata_test_lake".main.ducklake_table ORDER BY table_id;

-- Switch back to main and query
CALL ducklake_use_branch('test_lake', 'main');
SELECT * FROM test_lake.main.departments ORDER BY id;


-- Check table stats for both branches
SELECT * FROM "__ducklake_metadata_test_lake".main.ducklake_table_stats WHERE table_id = 1;



-- Switch to budget_branch and check what columns are actually being read
CALL ducklake_use_branch('test_lake', 'budget_branch');

-- Try selecting just the name column (which works correctly)
SELECT name, budget FROM test_lake.main.departments ORDER BY name;

-- Now try with id explicitly
SELECT id FROM test_lake.main.departments ORDER BY id;













