-- Worker V3: Add timestamps and priority columns
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_schema host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_schema');
USE t;
CALL ducklake_use_branch('t', 'schema_v3');

SELECT 'V3: Starting schema evolution...';

ALTER TABLE orders ADD COLUMN created_at TIMESTAMP;
SELECT 'V3: Added created_at column';

ALTER TABLE orders ADD COLUMN priority INT DEFAULT 1;
SELECT 'V3: Added priority column';

UPDATE orders SET priority = 3 WHERE amount > 200;
SELECT 'V3: Set high priority for large orders';

INSERT INTO orders (id, customer, amount, created_at, priority) VALUES (30, 'V3_Customer', 1000.00, '2024-01-15 10:30:00', 5);
SELECT 'V3: Inserted new high-priority order';

DELETE FROM orders WHERE id = 1;
SELECT 'V3: Deleted order 1';

SELECT 'V3: Final schema and data:';
SELECT * FROM orders ORDER BY id;
