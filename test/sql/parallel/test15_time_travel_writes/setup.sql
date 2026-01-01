-- Setup: History table with multiple snapshots
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_timetravel host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_timetravel');
USE t;

CREATE TABLE history (
    id INT,
    event VARCHAR,
    version INT
);

-- Create initial snapshots
INSERT INTO history VALUES (1, 'Initial', 1);
-- Snapshot 2

INSERT INTO history VALUES (2, 'Second', 1);
-- Snapshot 3

INSERT INTO history VALUES (3, 'Third', 1);
-- Snapshot 4

SELECT 'Main: Created history with ' || COUNT(*)::VARCHAR || ' rows across 4 snapshots' FROM history;

-- Create branches at current state
CALL ducklake_create_branch('t', 'writer_fast');
CALL ducklake_create_branch('t', 'writer_slow');
CALL ducklake_create_branch('t', 'reader_history');
CALL ducklake_create_branch('t', 'reader_branch');

SELECT 'Created 4 branches for time-travel test' as msg;
SELECT * FROM ducklake_snapshots('t');
