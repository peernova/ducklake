-- Worker B: Writes data while reading from A and main
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_cross_reads host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_cross_reads');
USE t;

CALL ducklake_use_branch('t', 'writer_b');
SELECT * FROM ducklake_current_branch('t');

-- Write 1
INSERT INTO metrics VALUES (201, 'branch_b', 5000, 1);
SELECT 'B: Inserted row 201';

-- Cross-read main
SELECT 'B: Main count = ' || COUNT(*)::VARCHAR FROM metrics AT (BRANCH => 'main');

-- Write 2
INSERT INTO metrics VALUES (202, 'branch_b', 6000, 2);
DELETE FROM metrics WHERE id = 2;
SELECT 'B: Deleted row 2 and inserted 202';

-- Cross-read A (should see A's fork-point state)
SELECT 'B: Writer_A count = ' || COUNT(*)::VARCHAR FROM metrics AT (BRANCH => 'writer_a');

-- Write 3
INSERT INTO metrics VALUES (203, 'branch_b', 7000, 3);
SELECT 'B: Inserted row 203';

SELECT 'B: Final own state:';
SELECT * FROM metrics ORDER BY id;
SELECT 'B: Total = ' || SUM(value)::VARCHAR FROM metrics;
