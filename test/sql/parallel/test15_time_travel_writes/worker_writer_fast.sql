-- Writer Fast: Rapid inserts creating multiple snapshots
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_timetravel host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_timetravel');
USE t;

CALL ducklake_use_branch('t', 'writer_fast');
SELECT 'WRITER_FAST: Rapid commits...';

INSERT INTO history VALUES (101, 'Fast_1', 2);
SELECT 'WRITER_FAST: Commit 1';

INSERT INTO history VALUES (102, 'Fast_2', 2);
SELECT 'WRITER_FAST: Commit 2';

INSERT INTO history VALUES (103, 'Fast_3', 2);
SELECT 'WRITER_FAST: Commit 3';

INSERT INTO history VALUES (104, 'Fast_4', 2);
SELECT 'WRITER_FAST: Commit 4';

INSERT INTO history VALUES (105, 'Fast_5', 2);
SELECT 'WRITER_FAST: Commit 5';

SELECT 'WRITER_FAST: Final count = ' || COUNT(*)::VARCHAR FROM history;
SELECT * FROM ducklake_snapshots('t');
