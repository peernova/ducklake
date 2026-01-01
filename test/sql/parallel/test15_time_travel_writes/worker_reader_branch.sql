-- Reader Branch: Reads across all branches
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_timetravel host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_timetravel');
USE t;

CALL ducklake_use_branch('t', 'reader_branch');
SELECT 'READER_BRANCH: Cross-branch reading...';

-- Read all branches
SELECT 'READER_BRANCH: Main = ' || COUNT(*)::VARCHAR FROM history AT (BRANCH => 'main');
SELECT 'READER_BRANCH: Writer_fast = ' || COUNT(*)::VARCHAR FROM history AT (BRANCH => 'writer_fast');
SELECT 'READER_BRANCH: Writer_slow = ' || COUNT(*)::VARCHAR FROM history AT (BRANCH => 'writer_slow');
SELECT 'READER_BRANCH: Reader_history = ' || COUNT(*)::VARCHAR FROM history AT (BRANCH => 'reader_history');

-- Add own data
INSERT INTO history VALUES (401, 'Reader_Branch', 4);
INSERT INTO history VALUES (402, 'Reader_Branch_2', 4);

SELECT 'READER_BRANCH: Own state:';
SELECT * FROM history ORDER BY id;
