-- Worker: DELETE half, INSERT new
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_schema_dml host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_schema_dml');
USE t;

CALL ducklake_use_branch('t', 'dml_deletes');
SELECT * FROM ducklake_current_branch('t');

SELECT 'DML_DELETES: Starting delete/insert cycle...';
SELECT 'DML_DELETES: Initial count = ' || COUNT(*)::VARCHAR FROM products;

-- Delete half the rows (odd IDs)
DELETE FROM products WHERE id % 2 = 1;
SELECT 'DML_DELETES: Deleted odd IDs';
SELECT 'DML_DELETES: After delete count = ' || COUNT(*)::VARCHAR FROM products;

-- Insert replacement products
INSERT INTO products VALUES
    (2001, 'Replacement_1', 111.11, 'replacement'),
    (2002, 'Replacement_2', 222.22, 'replacement'),
    (2003, 'Replacement_3', 333.33, 'replacement'),
    (2004, 'Replacement_4', 444.44, 'replacement'),
    (2005, 'Replacement_5', 555.55, 'replacement');
SELECT 'DML_DELETES: Inserted 5 replacements';

-- Delete one more
DELETE FROM products WHERE id = 2;
SELECT 'DML_DELETES: Deleted id=2';

SELECT 'DML_DELETES: Final count = ' || COUNT(*)::VARCHAR FROM products;
SELECT 'DML_DELETES: Category distribution:';
SELECT category, COUNT(*) as cnt FROM products GROUP BY category ORDER BY category;
