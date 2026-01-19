# DuckLake Access Events - OTel Infrastructure

OpenTelemetry-based audit logging infrastructure for DuckLake Service.

## Architecture

```
┌──────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  DuckLake UI │     │  OTel Collector │     │  ClickHouse     │
│  (Port 5173) │     │  (Port 4317/18) │     │  (Port 9000)    │
└──────┬───────┘     └────────▲────────┘     └────────▲────────┘
       │                      │                       │
       │ HTTP                 │ OTLP                  │ Native
       ▼                      │                       │
┌──────────────┐              │                       │
│  DuckLake BE │──────────────┴───────────────────────┘
│  (Port 8080) │     AUDIT logs → Filter → ClickHouse
└──────────────┘     Dashboard API → ClickHouse queries
```

## Quick Start

### 1. Start ClickHouse + OTel Collector

```bash
cd service/otel
docker compose --profile clickhouse up -d
```

### 2. Start the Backend

```bash
cd service/be

# Build first
./gradlew build -x test

# Option A: Gradle (dev mode)
./gradlew bootRun

# Option B: JAR (production)
java -jar build/libs/ducklake-service-0.0.1-SNAPSHOT.jar
```

### 3. Start the UI (optional)

```bash
cd service/ui

# Install dependencies (first time only)
npm install

# Start dev server
npm run dev
```

Open http://localhost:5173 → **Access Logs** tab

### 4. Verify

```bash
# Check services
docker ps | grep -E "(clickhouse|otel)"

# Generate test events
curl -s "http://localhost:8080/api/v1/catalogs" -H "X-User-Id: alice"

# Wait for batch (10s) then query ClickHouse
docker exec ducklake-clickhouse clickhouse-client \
  --query "SELECT * FROM otel_logs ORDER BY Timestamp DESC LIMIT 5"
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| DuckLake BE | 8080 | Spring Boot application |
| OTel Collector (gRPC) | 4317 | OTLP gRPC receiver |
| OTel Collector (HTTP) | 4318 | OTLP HTTP receiver |
| ClickHouse (Native) | 9000 | Native protocol |
| ClickHouse (HTTP) | 8123 | HTTP interface |
| Collector Health | 13133 | Health check endpoint |
| Collector zPages | 55679 | Debug UI |

## Backends

### ClickHouse (Recommended for Analytics)

Native OTel exporter - no loader needed.

```bash
# Start
docker compose --profile clickhouse up -d

# Query
docker exec ducklake-clickhouse clickhouse-client --query "
SELECT
    LogAttributes['user_id'] as user,
    LogAttributes['operation'] as op,
    LogAttributes['resource_type'] as resource,
    Timestamp
FROM otel_logs
ORDER BY Timestamp DESC
LIMIT 10"

# Stop
docker compose --profile clickhouse down
```

### PostgreSQL (Alternative)

Uses file exporter + Python loader.

```bash
# Start
docker compose --profile postgres up -d

# Load logs into PostgreSQL
pip install psycopg2-binary
python pg_loader.py --file data/audit_logs.json

# Watch mode (continuous loading)
python pg_loader.py --watch

# Query
psql -h localhost -p 5434 -U ducklake -d ducklake_service \
  -c "SELECT * FROM access_log ORDER BY timestamp DESC LIMIT 10"

# Stop
docker compose --profile postgres down
```

## Query Examples

### ClickHouse

```sql
-- Recent activity by user
SELECT
    LogAttributes['user_id'] as user,
    count() as events,
    max(Timestamp) as last_activity
FROM otel_logs
GROUP BY user
ORDER BY events DESC;

-- Events in last hour
SELECT * FROM otel_logs
WHERE Timestamp > now() - INTERVAL 1 HOUR
ORDER BY Timestamp DESC;

-- Filter by operation
SELECT * FROM otel_logs
WHERE LogAttributes['operation'] = 'select'
  AND LogAttributes['status'] = 'success';
```

### PostgreSQL

```sql
-- Recent activity
SELECT user_id, resource_type, operation, timestamp
FROM access_log
ORDER BY timestamp DESC
LIMIT 10;

-- Query raw OTLP data
SELECT
    raw_data->'parsed_attributes'->>'thread.name' as thread,
    raw_data->'resource_attributes'->>'service.name' as service
FROM access_log
WHERE raw_data IS NOT NULL;
```

## Configuration Files

| File | Description |
|------|-------------|
| `docker-compose.yaml` | Docker services (ClickHouse, PostgreSQL, Collector) |
| `otel-collector-clickhouse.yaml` | Collector config for ClickHouse backend |
| `otel-collector-postgres.yaml` | Collector config for PostgreSQL backend |
| `init-clickhouse.sql` | ClickHouse schema initialization |
| `init-postgres.sql` | PostgreSQL schema initialization |
| `pg_loader.py` | Python script to load OTLP JSON into PostgreSQL |
| `clickhouse-users.xml` | ClickHouse user permissions |

## Troubleshooting

### Check Collector Logs
```bash
docker logs ducklake-otel-clickhouse -f
```

### Check ClickHouse Connection
```bash
docker exec ducklake-clickhouse clickhouse-client --query "SELECT 1"
```

### Verify AUDIT Logs are Filtered
```bash
# Should see logback.marker: ["AUDIT"] in output
docker logs ducklake-otel-clickhouse 2>&1 | grep -i audit
```

### Reset ClickHouse Data
```bash
docker compose --profile clickhouse down -v
docker compose --profile clickhouse up -d
```

## Dashboard API

The BE provides dashboard endpoints that query ClickHouse:

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/dashboard/stats` | Summary stats (total events, unique users, etc.) |
| `GET /api/v1/dashboard/recent?limit=20` | Recent events |
| `GET /api/v1/dashboard/events-by-user?limit=10` | Events grouped by user |
| `GET /api/v1/dashboard/events-by-resource` | Events grouped by resource type |
| `GET /api/v1/dashboard/operations` | Events grouped by operation and status |
| `GET /api/v1/dashboard/timeline?hours=24` | Events over time for charting |

Example:
```bash
curl http://localhost:8080/api/v1/dashboard/stats
# {"total_events":41,"unique_users":4,"events_today":41,"events_last_hour":5}
```

## UI Dashboard

The UI includes an **Access Logs** page with:
- Real-time stats cards (total events, unique users, events today/hour)
- Recent events table
- Top users breakdown
- Events by resource type
- Operations breakdown

Auto-refreshes every 30 seconds.
