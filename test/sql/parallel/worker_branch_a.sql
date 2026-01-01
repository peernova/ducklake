-- Worker A: INSERT + UPDATE operations on branch_a
-- Run this in a separate terminal in parallel with worker_b and worker_c
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_data');
USE t;
CALL ducklake_use_branch('t', 'branch_a');

SELECT 'Branch A: Starting with ' || COUNT(*)::VARCHAR || ' users' FROM users;

INSERT INTO users VALUES (10, 'BranchA_User1', 'active');
SELECT 'Branch A: Inserted user 10';

UPDATE users SET status = 'premium' WHERE id = 1;
SELECT 'Branch A: Updated user 1 to premium';

INSERT INTO users VALUES (11, 'BranchA_User2', 'active');
SELECT 'Branch A: Inserted user 11';

SELECT 'Branch A: Final count = ' || COUNT(*)::VARCHAR FROM users;
SELECT * FROM users ORDER BY id;
