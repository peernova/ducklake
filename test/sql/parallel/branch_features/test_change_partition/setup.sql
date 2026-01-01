-- Setup: Test changing partition key on different branches
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=change_partition_test host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/change_partition_test');
USE t;

-- Create events table
CREATE TABLE events (
    id INTEGER,
    event_type VARCHAR,
    region VARCHAR,
    event_date DATE,
    amount DECIMAL(10,2)
);

-- Initially partition by event_type
ALTER TABLE events SET PARTITIONED BY (event_type);

-- Insert initial data
INSERT INTO events VALUES
    (1, 'sale', 'US', '2024-01-15', 100.00),
    (2, 'sale', 'EU', '2024-01-16', 200.00),
    (3, 'refund', 'US', '2024-01-17', 50.00),
    (4, 'sale', 'ASIA', '2024-01-18', 150.00),
    (5, 'refund', 'EU', '2024-01-19', 75.00),
    (6, 'sale', 'US', '2024-01-20', 300.00);

SELECT 'Main branch: 6 events partitioned by event_type';
SELECT event_type, COUNT(*) as count FROM events GROUP BY event_type ORDER BY event_type;

-- Create branch that changes partition to region
CALL ducklake_create_branch('t', 'partition_by_region');
CALL ducklake_use_branch('t', 'partition_by_region');
ALTER TABLE events SET PARTITIONED BY (region);
SELECT 'partition_by_region: Changed partition key to region';

-- Create branch that changes partition to date
CALL ducklake_use_branch('t', 'main');
CALL ducklake_create_branch('t', 'partition_by_date');
CALL ducklake_use_branch('t', 'partition_by_date');
ALTER TABLE events SET PARTITIONED BY (event_date);
SELECT 'partition_by_date: Changed partition key to event_date';

-- Switch back to main
CALL ducklake_use_branch('t', 'main');
SELECT 'Setup complete. Branches: main (by event_type), partition_by_region, partition_by_date';
