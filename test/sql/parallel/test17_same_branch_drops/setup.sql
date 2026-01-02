-- Setup: Create many schemas, tables, views for parallel DROP testing
ATTACH 'ducklake:metadata=postgres:host=localhost port=5433 dbname=parallel_same_drop' AS t (DATA_PATH '/tmp/parallel_same_drop');
USE t;

-- Schema 1: tables_only (for worker1 to drop tables)
CREATE SCHEMA tables_only;
CREATE TABLE tables_only.t1 (id INT, val VARCHAR);
CREATE TABLE tables_only.t2 (id INT, val VARCHAR);
CREATE TABLE tables_only.t3 (id INT, val VARCHAR);
CREATE TABLE tables_only.t4 (id INT, val VARCHAR);
INSERT INTO tables_only.t1 VALUES (1, 'one');
INSERT INTO tables_only.t2 VALUES (2, 'two');
INSERT INTO tables_only.t3 VALUES (3, 'three');
INSERT INTO tables_only.t4 VALUES (4, 'four');

-- Schema 2: views_schema (for worker2 to drop views)
CREATE SCHEMA views_schema;
CREATE TABLE views_schema.base_data (id INT, name VARCHAR, value INT);
INSERT INTO views_schema.base_data VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30);

CREATE VIEW views_schema.v1 AS SELECT * FROM views_schema.base_data WHERE id = 1;
CREATE VIEW views_schema.v2 AS SELECT * FROM views_schema.base_data WHERE id = 2;
CREATE VIEW views_schema.v3 AS SELECT * FROM views_schema.base_data WHERE id = 3;
CREATE VIEW views_schema.v_all AS SELECT * FROM views_schema.base_data;

-- Schema 3-5: for worker3 to drop entire schemas
CREATE SCHEMA drop_me_1;
CREATE TABLE drop_me_1.data (id INT);
INSERT INTO drop_me_1.data VALUES (1);
CREATE VIEW drop_me_1.view1 AS SELECT * FROM drop_me_1.data;

CREATE SCHEMA drop_me_2;
CREATE TABLE drop_me_2.data (id INT);
INSERT INTO drop_me_2.data VALUES (2);

CREATE SCHEMA drop_me_3;
CREATE TABLE drop_me_3.data (id INT);
INSERT INTO drop_me_3.data VALUES (3);
CREATE VIEW drop_me_3.view1 AS SELECT * FROM drop_me_3.data;
CREATE VIEW drop_me_3.view2 AS SELECT * FROM drop_me_3.data;

-- Schema 6: keep_me (should survive all drops)
CREATE SCHEMA keep_me;
CREATE TABLE keep_me.important (id INT, data VARCHAR);
INSERT INTO keep_me.important VALUES (1, 'must survive');

SELECT 'Setup complete' as msg;
SELECT schema_name FROM information_schema.schemata
WHERE catalog_name = 't' AND schema_name NOT IN ('information_schema', 'pg_catalog')
ORDER BY schema_name;

-- Create dev branch for parallel testing
CALL ducklake_create_branch('t', 'dev');
SELECT 'Created dev branch for parallel DROP testing' as msg;
