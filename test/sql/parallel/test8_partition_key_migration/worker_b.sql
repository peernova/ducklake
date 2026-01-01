-- Worker B: Moves 'processing' orders to 'shipped'
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_partition_migration host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_partition_migration');
USE t;

CALL ducklake_use_branch('t', 'migrate_b');
SELECT * FROM ducklake_current_branch('t');

SELECT 'MIGRATE_B: Before - partition distribution:';
SELECT status, COUNT(*) as cnt FROM orders GROUP BY status ORDER BY status;

-- Move processing to shipped (partition key change!)
UPDATE orders SET status = 'shipped' WHERE status = 'processing';
SELECT 'MIGRATE_B: Moved processing -> shipped';

SELECT 'MIGRATE_B: After - partition distribution:';
SELECT status, COUNT(*) as cnt FROM orders GROUP BY status ORDER BY status;
