-- Worker A: Writes data while reading from B and main
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_cross_reads host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_cross_reads');
USE t;

CALL ducklake_use_branch('t', 'writer_a');
SELECT * FROM ducklake_current_branch('t');

-- Write 1
INSERT INTO metrics VALUES (101, 'branch_a', 1000, 1);
SELECT 'A: Inserted row 101';

-- Cross-read main
SELECT 'A: Main count = ' || COUNT(*)::VARCHAR FROM metrics AT (BRANCH => 'main');

-- Write 2
INSERT INTO metrics VALUES (102, 'branch_a', 2000, 2);
UPDATE metrics SET value = value + 100 WHERE source = 'main';
SELECT 'A: Updated main rows and inserted 102';

-- Cross-read B (should see B's fork-point state)
SELECT 'A: Writer_B count = ' || COUNT(*)::VARCHAR FROM metrics AT (BRANCH => 'writer_b');

-- Write 3
INSERT INTO metrics VALUES (103, 'branch_a', 3000, 3);
SELECT 'A: Inserted row 103';

SELECT 'A: Final own state:';
SELECT * FROM metrics ORDER BY id;
SELECT 'A: Total = ' || SUM(value)::VARCHAR FROM metrics;
