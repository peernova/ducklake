-- ClickHouse initialization for DuckLake Access Events
-- This script is run automatically on container startup

-- Access log table with MergeTree engine
-- Optimized for time-series queries and high write throughput
CREATE TABLE IF NOT EXISTS access_log (
    event_id String,
    timestamp DateTime64(3) DEFAULT now64(3),
    trace_id String DEFAULT '',
    span_id String DEFAULT '',
    user_id String,
    user_email String DEFAULT '',
    source_ip String DEFAULT '',
    client_info String DEFAULT '',
    resource_type LowCardinality(String),
    resource_id String,
    resource_name String DEFAULT '',
    resource_path String DEFAULT '{}',
    operation LowCardinality(String) DEFAULT '',
    status LowCardinality(String) DEFAULT '',
    rejection_reason String DEFAULT '',
    rows_affected UInt64 DEFAULT 0,
    execution_time_ms UInt32 DEFAULT 0
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (user_id, resource_type, timestamp)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Secondary index for resource-based queries
ALTER TABLE access_log ADD INDEX idx_resource (resource_type, resource_id) TYPE minmax GRANULARITY 4;

-- Secondary index for trace correlation
ALTER TABLE access_log ADD INDEX idx_trace (trace_id) TYPE bloom_filter GRANULARITY 4;

-- Favorites table using ReplacingMergeTree for upsert behavior
CREATE TABLE IF NOT EXISTS favorites (
    user_id String,
    resource_type LowCardinality(String),
    resource_id String,
    resource_name String DEFAULT '',
    resource_path String DEFAULT '{}',
    display_name String DEFAULT '',
    created_at DateTime64(3) DEFAULT now64(3),
    updated_at DateTime64(3) DEFAULT now64(3),
    is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (user_id, resource_type, resource_id)
SETTINGS index_granularity = 8192;

-- Materialized view: recent activity per user (auto-updated)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_recent_activity
ENGINE = ReplacingMergeTree(timestamp)
ORDER BY (user_id, resource_type, resource_id)
AS SELECT
    user_id,
    resource_type,
    resource_id,
    argMax(event_id, timestamp) as event_id,
    max(timestamp) as timestamp,
    argMax(trace_id, timestamp) as trace_id,
    argMax(span_id, timestamp) as span_id,
    argMax(user_email, timestamp) as user_email,
    argMax(source_ip, timestamp) as source_ip,
    argMax(client_info, timestamp) as client_info,
    argMax(resource_name, timestamp) as resource_name,
    argMax(resource_path, timestamp) as resource_path,
    argMax(operation, timestamp) as operation,
    argMax(status, timestamp) as status,
    argMax(rejection_reason, timestamp) as rejection_reason,
    argMax(rows_affected, timestamp) as rows_affected,
    argMax(execution_time_ms, timestamp) as execution_time_ms
FROM access_log
GROUP BY user_id, resource_type, resource_id;

-- Materialized view: hourly statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hourly_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, resource_type, status)
AS SELECT
    toStartOfHour(timestamp) as hour,
    resource_type,
    status,
    count() as event_count,
    uniqExact(user_id) as unique_users,
    uniqExact(resource_id) as unique_resources,
    sum(execution_time_ms) as total_execution_ms,
    max(execution_time_ms) as max_execution_ms
FROM access_log
GROUP BY hour, resource_type, status;

-- Materialized view: daily user activity
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_user_activity
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, user_id, resource_type)
AS SELECT
    toDate(timestamp) as day,
    user_id,
    resource_type,
    count() as event_count,
    uniqExact(resource_id) as unique_resources,
    countIf(status = 'rejected') as rejections
FROM access_log
GROUP BY day, user_id, resource_type;

-- Helper: active favorites view (excludes soft-deleted)
CREATE VIEW IF NOT EXISTS v_favorites AS
SELECT * FROM favorites FINAL WHERE is_deleted = 0;

-- System settings for better performance
-- Run these manually if needed:
-- SET max_insert_threads = 4;
-- SET async_insert = 1;
-- SET wait_for_async_insert = 0;
