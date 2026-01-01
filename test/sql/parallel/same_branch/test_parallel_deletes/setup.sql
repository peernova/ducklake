-- Setup: Create table with 20 rows (5 per worker to delete)
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=same_branch_deletes host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/same_branch_deletes');
USE t;

CREATE TABLE data (id INT, category VARCHAR, value INT);
-- Worker 1 will delete category='A' (ids 1-5)
INSERT INTO data VALUES (1, 'A', 10), (2, 'A', 20), (3, 'A', 30), (4, 'A', 40), (5, 'A', 50);
-- Worker 2 will delete category='B' (ids 6-10)
INSERT INTO data VALUES (6, 'B', 10), (7, 'B', 20), (8, 'B', 30), (9, 'B', 40), (10, 'B', 50);
-- Worker 3 will delete category='C' (ids 11-15)
INSERT INTO data VALUES (11, 'C', 10), (12, 'C', 20), (13, 'C', 30), (14, 'C', 40), (15, 'C', 50);
-- Worker 4 will delete category='D' (ids 16-20)
INSERT INTO data VALUES (16, 'D', 10), (17, 'D', 20), (18, 'D', 30), (19, 'D', 40), (20, 'D', 50);
-- Keep category='Z' (ids 21-25) - should not be deleted
INSERT INTO data VALUES (21, 'Z', 10), (22, 'Z', 20), (23, 'Z', 30), (24, 'Z', 40), (25, 'Z', 50);

SELECT 'Setup complete - 25 rows (5 per category A,B,C,D,Z)';
SELECT category, COUNT(*) as cnt FROM data GROUP BY category ORDER BY category;
