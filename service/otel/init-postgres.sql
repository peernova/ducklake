-- PostgreSQL initialization for DuckLake Access Events
-- This script is run automatically on container startup

-- Access log table (main event store)
CREATE TABLE IF NOT EXISTS access_log (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(36) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    trace_id VARCHAR(32),
    span_id VARCHAR(16),
    user_id VARCHAR(255) NOT NULL,
    user_email VARCHAR(255),
    source_ip VARCHAR(45),
    client_info VARCHAR(500),
    resource_type VARCHAR(50) NOT NULL,
    resource_id VARCHAR(500) NOT NULL,
    resource_name VARCHAR(500),
    resource_path JSONB,
    operation VARCHAR(50),
    status VARCHAR(20),
    rejection_reason VARCHAR(500),
    rows_affected BIGINT,
    execution_time_ms INT,
    raw_data JSONB  -- Raw OTLP log record for future improvements
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_access_log_user_time
    ON access_log(user_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_access_log_resource
    ON access_log(resource_type, resource_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_access_log_timestamp
    ON access_log(timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_access_log_trace
    ON access_log(trace_id) WHERE trace_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_access_log_status
    ON access_log(status, timestamp DESC);

-- GIN index for JSONB path queries
CREATE INDEX IF NOT EXISTS idx_access_log_path
    ON access_log USING GIN(resource_path);

-- Favorites table
CREATE TABLE IF NOT EXISTS favorites (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    resource_type VARCHAR(50) NOT NULL,
    resource_id VARCHAR(500) NOT NULL,
    resource_name VARCHAR(500),
    resource_path JSONB,
    display_name VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, resource_type, resource_id)
);

CREATE INDEX IF NOT EXISTS idx_favorites_user
    ON favorites(user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_favorites_user_type
    ON favorites(user_id, resource_type, created_at DESC);

-- Helpful view: recent activity per user (deduplicated)
CREATE OR REPLACE VIEW recent_activity AS
SELECT DISTINCT ON (user_id, resource_type, resource_id)
    event_id, timestamp, trace_id, span_id,
    user_id, user_email, source_ip, client_info,
    resource_type, resource_id, resource_name, resource_path,
    operation, status, rejection_reason, rows_affected, execution_time_ms
FROM access_log
ORDER BY user_id, resource_type, resource_id, timestamp DESC;

-- Helpful view: audit statistics
CREATE OR REPLACE VIEW audit_stats AS
SELECT
    resource_type,
    COUNT(*) as total_events,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT resource_id) as unique_resources,
    COUNT(*) FILTER (WHERE status = 'rejected') as rejections,
    AVG(execution_time_ms) FILTER (WHERE execution_time_ms IS NOT NULL) as avg_execution_ms,
    MAX(timestamp) as last_activity
FROM access_log
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY resource_type;

-- Retention policy: auto-delete old events (optional)
-- Run this as a cron job or pg_cron extension
-- DELETE FROM access_log WHERE timestamp < NOW() - INTERVAL '90 days';

COMMENT ON TABLE access_log IS 'Access events for auditing and recent activity tracking';
COMMENT ON TABLE favorites IS 'User-favorited resources for quick access';
