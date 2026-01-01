-- Worker D: Moves ALL orders to 'archived'
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_partition_migration host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_partition_migration');
USE t;

CALL ducklake_use_branch('t', 'migrate_d');
SELECT * FROM ducklake_current_branch('t');

SELECT 'MIGRATE_D: Before - partition distribution:';
SELECT status, COUNT(*) as cnt FROM orders GROUP BY status ORDER BY status;

-- Move ALL to archived (mass partition migration!)
UPDATE orders SET status = 'archived';
SELECT 'MIGRATE_D: Moved ALL -> archived';

SELECT 'MIGRATE_D: After - partition distribution:';
SELECT status, COUNT(*) as cnt FROM orders GROUP BY status ORDER BY status;
