-- Worker B: DELETE + INSERT operations on branch_b
-- Run this in a separate terminal in parallel with worker_a and worker_c
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_data');
USE t;
CALL ducklake_use_branch('t', 'branch_b');

SELECT 'Branch B: Starting with ' || COUNT(*)::VARCHAR || ' users' FROM users;

DELETE FROM users WHERE id = 2;
SELECT 'Branch B: Deleted user 2';

INSERT INTO users VALUES (20, 'BranchB_User1', 'vip');
SELECT 'Branch B: Inserted user 20';

INSERT INTO users VALUES (21, 'BranchB_User2', 'vip');
SELECT 'Branch B: Inserted user 21';

SELECT 'Branch B: Final count = ' || COUNT(*)::VARCHAR FROM users;
SELECT * FROM users ORDER BY id;
