-- Setup: Events table with 15 country partitions
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/postgres_scanner/build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD '/Users/nareshbalusu/Downloads/claude/ducklake/ducklake_branching/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=parallel_high_card host=localhost port=5433 user=postgres password=postgres' AS t (DATA_PATH '/tmp/parallel_high_card');
USE t;

CREATE TABLE events (
    id INT,
    country VARCHAR,
    event_type VARCHAR,
    value INT
);

ALTER TABLE events SET PARTITIONED BY (country);

-- Insert data for 15 countries
INSERT INTO events VALUES
    (1, 'US', 'click', 100), (2, 'US', 'view', 50),
    (3, 'CA', 'click', 80), (4, 'CA', 'view', 40),
    (5, 'MX', 'click', 60), (6, 'MX', 'view', 30),
    (7, 'BR', 'click', 90), (8, 'BR', 'view', 45),
    (9, 'AR', 'click', 70), (10, 'AR', 'view', 35),
    (11, 'UK', 'click', 110), (12, 'UK', 'view', 55),
    (13, 'DE', 'click', 120), (14, 'DE', 'view', 60),
    (15, 'FR', 'click', 100), (16, 'FR', 'view', 50),
    (17, 'IT', 'click', 85), (18, 'IT', 'view', 42),
    (19, 'ES', 'click', 75), (20, 'ES', 'view', 37),
    (21, 'JP', 'click', 150), (22, 'JP', 'view', 75),
    (23, 'CN', 'click', 200), (24, 'CN', 'view', 100),
    (25, 'IN', 'click', 180), (26, 'IN', 'view', 90),
    (27, 'AU', 'click', 95), (28, 'AU', 'view', 47),
    (29, 'KR', 'click', 130), (30, 'KR', 'view', 65);

SELECT 'Main: Created events with ' || COUNT(*)::VARCHAR || ' rows across 15 countries' FROM events;
SELECT country, COUNT(*) as cnt FROM events GROUP BY country ORDER BY country;

CALL ducklake_create_branch('t', 'americas');
CALL ducklake_create_branch('t', 'europe');
CALL ducklake_create_branch('t', 'asiapac');
CALL ducklake_create_branch('t', 'global_ops');
CALL ducklake_create_branch('t', 'new_regions');

SELECT 'Created 5 regional branches' as msg;
