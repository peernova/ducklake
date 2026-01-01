-- Worker Reader: Only reads from all branches continuously
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_cross_reads host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_cross_reads');
USE t;

CALL ducklake_use_branch('t', 'reader_only');
SELECT * FROM ducklake_current_branch('t');

-- Multiple read passes
SELECT 'READER: Pass 1 - Reading all branches';
SELECT 'READER: Main = ' || COUNT(*)::VARCHAR || ', sum=' || SUM(value)::VARCHAR FROM metrics AT (BRANCH => 'main');
SELECT 'READER: Writer_A = ' || COUNT(*)::VARCHAR FROM metrics AT (BRANCH => 'writer_a');
SELECT 'READER: Writer_B = ' || COUNT(*)::VARCHAR FROM metrics AT (BRANCH => 'writer_b');

-- Insert something on reader branch
INSERT INTO metrics VALUES (301, 'reader', 9999, 1);
SELECT 'READER: Added own row 301';

SELECT 'READER: Pass 2 - Reading again';
SELECT 'READER: Main = ' || COUNT(*)::VARCHAR || ', sum=' || SUM(value)::VARCHAR FROM metrics AT (BRANCH => 'main');
SELECT 'READER: Writer_A = ' || COUNT(*)::VARCHAR FROM metrics AT (BRANCH => 'writer_a');
SELECT 'READER: Writer_B = ' || COUNT(*)::VARCHAR FROM metrics AT (BRANCH => 'writer_b');

SELECT 'READER: Own state:';
SELECT * FROM metrics ORDER BY id;
