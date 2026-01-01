-- Writer Slow: Updates and deletes
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_timetravel host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_timetravel');
USE t;

CALL ducklake_use_branch('t', 'writer_slow');
SELECT 'WRITER_SLOW: Slow updates...';

UPDATE history SET version = 10 WHERE id = 1;
SELECT 'WRITER_SLOW: Updated id=1';

DELETE FROM history WHERE id = 2;
SELECT 'WRITER_SLOW: Deleted id=2';

INSERT INTO history VALUES (201, 'Slow_1', 10);
SELECT 'WRITER_SLOW: Inserted 201';

UPDATE history SET event = 'Modified_' || event WHERE version = 1;
SELECT 'WRITER_SLOW: Updated remaining v1 rows';

SELECT 'WRITER_SLOW: Final state:';
SELECT * FROM history ORDER BY id;
