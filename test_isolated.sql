LOAD 'build/release/extension/ducklake/ducklake.duckdb_extension';



-- Clean slate
ATTACH 'ducklake:test_isolated.ducklake' AS test_lake;

CREATE TABLE test_lake.main.departments (id INT, name VARCHAR, budget DECIMAL(15,2));
INSERT INTO test_lake.main.departments VALUES 
    (1, 'Engineering', 1000000.00),
    (2, 'Sales', 500000.00),
    (3, 'Marketing', 300000.00);

CALL ducklake_create_branch('test_lake', 'budget_branch');
CALL ducklake_use_branch('test_lake', 'budget_branch');

INSERT INTO test_lake.main.departments VALUES (4, 'HR', 200000.00);
UPDATE test_lake.main.departments SET budget = 1500000.00 WHERE name = 'Engineering';

SELECT 'Query with WHERE clause:' as info;
SELECT * FROM test_lake.main.departments WHERE name = 'Engineering';

SELECT 'Query without WHERE clause:' as info;
SELECT * FROM test_lake.main.departments ORDER BY id;



SELECT branch_id, data_file_id, row_id_start, record_count
FROM "__ducklake_metadata_test_lake".main.ducklake_data_file
WHERE table_id = 1
ORDER BY branch_id, data_file_id;


-- List all parquet files
SELECT 'Files in directory:' as info;
SELECT * FROM glob('test_isolated.ducklake.files/main/departments/*.parquet');

-- Check each file separately
SELECT 'File contents:' as info;
SELECT filename, * FROM parquet_scan('test_isolated.ducklake.files/main/departments/*.parquet', filename=true);


SELECT branch_id, data_file_id, path, record_count
FROM "__ducklake_metadata_test_isolated".main.ducklake_data_file
ORDER BY branch_id, data_file_id;


-- Check the column definitions
SELECT column_id, column_name, column_type, column_order
FROM "__ducklake_metadata_test_lake".main.ducklake_column
WHERE table_id = 1
ORDER BY column_order;


SELECT * FROM parquet_schema('test_isolated.ducklake.files/main/departments/ducklake-019b5c97-b16d-761c-8b8d-1c140af27346.parquet');







