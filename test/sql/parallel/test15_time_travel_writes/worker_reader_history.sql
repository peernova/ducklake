-- Reader History: Reads historical snapshots on main
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_timetravel host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_timetravel');
USE t;

CALL ducklake_use_branch('t', 'reader_history');
SELECT 'READER_HISTORY: Reading historical snapshots on main...';

-- Read current main state
SELECT 'READER_HISTORY: Main current = ' || COUNT(*)::VARCHAR FROM history AT (BRANCH => 'main');

-- Read writer_fast (may be in progress)
SELECT 'READER_HISTORY: Writer_fast current = ' || COUNT(*)::VARCHAR FROM history AT (BRANCH => 'writer_fast');

-- Read writer_slow (may be in progress)
SELECT 'READER_HISTORY: Writer_slow current = ' || COUNT(*)::VARCHAR FROM history AT (BRANCH => 'writer_slow');

-- Add own data
INSERT INTO history VALUES (301, 'Reader_History', 3);

SELECT 'READER_HISTORY: Own state:';
SELECT * FROM history ORDER BY id;
