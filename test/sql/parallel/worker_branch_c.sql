-- Worker C: Schema change + INSERT operations on branch_c
-- Run this in a separate terminal in parallel with worker_a and worker_b
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_data');
USE t;
CALL ducklake_use_branch('t', 'branch_c');

SELECT 'Branch C: Starting with ' || COUNT(*)::VARCHAR || ' users' FROM users;

ALTER TABLE users ADD COLUMN email VARCHAR;
SELECT 'Branch C: Added email column';

UPDATE users SET email = name || '@example.com';
SELECT 'Branch C: Updated emails';

INSERT INTO users VALUES (30, 'BranchC_User1', 'active', 'branchc1@example.com');
SELECT 'Branch C: Inserted user 30';

SELECT 'Branch C: Final count = ' || COUNT(*)::VARCHAR FROM users;
SELECT * FROM users ORDER BY id;
