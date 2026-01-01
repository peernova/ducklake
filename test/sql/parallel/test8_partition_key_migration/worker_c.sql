-- Worker C: Moves 'shipped' orders to 'delivered'
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_partition_migration host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_partition_migration');
USE t;

CALL ducklake_use_branch('t', 'migrate_c');
SELECT * FROM ducklake_current_branch('t');

SELECT 'MIGRATE_C: Before - partition distribution:';
SELECT status, COUNT(*) as cnt FROM orders GROUP BY status ORDER BY status;

-- Move shipped to delivered (partition key change!)
UPDATE orders SET status = 'delivered' WHERE status = 'shipped';
SELECT 'MIGRATE_C: Moved shipped -> delivered';

SELECT 'MIGRATE_C: After - partition distribution:';
SELECT status, COUNT(*) as cnt FROM orders GROUP BY status ORDER BY status;
