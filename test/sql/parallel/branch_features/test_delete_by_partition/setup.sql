-- Setup: Test deleting data by partition value on different branches
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=delete_partition_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/delete_partition_test');
USE t;

-- Create logs table partitioned by severity
CREATE TABLE logs (
    id INTEGER,
    message VARCHAR,
    severity VARCHAR,
    timestamp TIMESTAMP
);

ALTER TABLE logs SET PARTITIONED BY (severity);

-- Insert initial data across severity levels
INSERT INTO logs VALUES
    (1, 'System started', 'INFO', '2024-01-15 08:00:00'),
    (2, 'User login', 'INFO', '2024-01-15 08:05:00'),
    (3, 'Connection timeout', 'WARNING', '2024-01-15 08:10:00'),
    (4, 'Disk full', 'ERROR', '2024-01-15 08:15:00'),
    (5, 'Database connection', 'INFO', '2024-01-15 08:20:00'),
    (6, 'Memory low', 'WARNING', '2024-01-15 08:25:00'),
    (7, 'Authentication failed', 'ERROR', '2024-01-15 08:30:00'),
    (8, 'Session timeout', 'WARNING', '2024-01-15 08:35:00'),
    (9, 'System crash', 'CRITICAL', '2024-01-15 08:40:00'),
    (10, 'Recovery started', 'INFO', '2024-01-15 08:45:00'),
    (11, 'Backup complete', 'INFO', '2024-01-15 08:50:00'),
    (12, 'Network error', 'ERROR', '2024-01-15 08:55:00');

SELECT 'Main branch: 12 logs across 4 severity levels';
SELECT severity, COUNT(*) as count FROM logs GROUP BY severity ORDER BY severity;

-- Create branch that deletes all INFO logs
CALL ducklake_create_branch('t', 'no_info');
CALL ducklake_use_branch('t', 'no_info');
DELETE FROM logs WHERE severity = 'INFO';
SELECT 'no_info: Deleted INFO partition (5 rows deleted)';

-- Create branch that deletes all ERROR and WARNING logs
CALL ducklake_use_branch('t', 'main');
CALL ducklake_create_branch('t', 'clean_logs');
CALL ducklake_use_branch('t', 'clean_logs');
DELETE FROM logs WHERE severity IN ('ERROR', 'WARNING');
SELECT 'clean_logs: Deleted ERROR and WARNING partitions (6 rows deleted)';

-- Create branch that keeps only CRITICAL logs
CALL ducklake_use_branch('t', 'main');
CALL ducklake_create_branch('t', 'critical_only');
CALL ducklake_use_branch('t', 'critical_only');
DELETE FROM logs WHERE severity != 'CRITICAL';
SELECT 'critical_only: Deleted all except CRITICAL (11 rows deleted)';

-- Switch back to main
CALL ducklake_use_branch('t', 'main');
SELECT 'Setup complete. Branches: main, no_info, clean_logs, critical_only';
