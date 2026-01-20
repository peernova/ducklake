# DuckLake Service

Web service and UI for DuckLake catalog management with access logging and analytics.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────┐
│   React UI  │────▶│ Spring Boot │────▶│    DuckDB +     │
│  (Vite)     │     │   Backend   │     │    DuckLake     │
└─────────────┘     └──────┬──────┘     └─────────────────┘
                           │
                           │ OTLP (logs)
                           ▼
                    ┌─────────────┐
                    │    OTel     │
                    │  Collector  │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ClickHouse│ │  Jaeger  │ │  Debug   │
        │  (logs)  │ │ (traces) │ │  (stdout)│
        └──────────┘ └──────────┘ └──────────┘
```

## Quick Start

### 1. Start Infrastructure (ClickHouse + OTel Collector + Jaeger)

```bash
cd service/otel
docker compose --profile clickhouse up -d
```

This starts:
- **ClickHouse** (port 8123 HTTP, 9000 native) - stores access logs
- **OTel Collector** (port 4317 gRPC, 4318 HTTP) - receives and routes telemetry
- **Jaeger** (port 16686 UI) - distributed tracing visualization

Verify services are running:
```bash
docker ps --filter "name=ducklake"
```

### 2. Start Backend (Spring Boot)

#### Option A: Without OpenTelemetry (simpler, no trace correlation)
```bash
cd service/be
./gradlew bootRun
```

#### Option B: With OpenTelemetry (recommended, full tracing)
```bash
cd service/be

# Download OTel agent if not present
[ -f opentelemetry-javaagent.jar ] || \
  curl -L -o opentelemetry-javaagent.jar \
  https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar

# Run with OTel agent
./run-with-otel.sh
```

Or manually:
```bash
java -javaagent:opentelemetry-javaagent.jar \
     -Dotel.service.name=ducklake-service \
     -Dotel.exporter.otlp.endpoint=http://localhost:4317 \
     -Dotel.logs.exporter=otlp \
     -Dotel.metrics.exporter=none \
     -jar build/libs/ducklake-service-*.jar
```

Backend runs on **http://localhost:8080**

### 3. Start UI (React + Vite)

```bash
cd service/ui
npm install
npm run dev
```

UI runs on **http://localhost:5173**

## Service URLs

| Service | URL | Description |
|---------|-----|-------------|
| UI | http://localhost:5173 | React dashboard |
| Backend API | http://localhost:8080/api/v1 | REST API |
| Swagger UI | http://localhost:8080/swagger-ui.html | API documentation |
| Jaeger UI | http://localhost:16686 | Distributed tracing |
| ClickHouse | http://localhost:8123 | HTTP interface |

## Access Logging

When running with OTel, all resource access is logged to ClickHouse:

### What Gets Logged

Each query to `/api/v1/query` generates events for:
- **api** - The API endpoint accessed
- **catalog** - Each catalog accessed
- **branch** - Each branch accessed
- **schema** - Each schema accessed
- **table** - Each table accessed (with columns)

All events from one request share the same `trace_id` for correlation.

### Viewing Logs

1. **UI Dashboard**: Navigate to "Resource Access Log" in the sidebar
2. **ClickHouse direct**:
   ```bash
   docker exec ducklake-clickhouse clickhouse-client \
     --query "SELECT * FROM otel_logs ORDER BY Timestamp DESC LIMIT 10"
   ```
3. **Jaeger**: Click "Traces ↗" link in the UI to view distributed traces

### Query Logs by Trace ID

```sql
SELECT
    Timestamp,
    LogAttributes['resource_type'] as resource_type,
    LogAttributes['resource_id'] as resource_id,
    LogAttributes['operation'] as operation
FROM otel_logs
WHERE LogAttributes['trace_id'] = 'your-trace-id-here'
ORDER BY Timestamp
```

## Configuration

### Backend (`application.yml`)

```yaml
# ClickHouse connection for reading logs
clickhouse:
  url: jdbc:clickhouse://localhost:8123/default
  enabled: true

# OpenTelemetry (set via environment or system properties)
otel:
  service.name: ducklake-service
  exporter.otlp.endpoint: http://localhost:4317
```

### OTel Collector (`otel-collector-clickhouse.yaml`)

Key pipelines:
- **logs**: OTLP → filter (AUDIT only) → ClickHouse + debug
- **traces**: OTLP → Jaeger + debug
- **metrics**: OTLP → debug

## Stopping Services

```bash
# Stop infrastructure
cd service/otel
docker compose --profile clickhouse down

# Stop backend: Ctrl+C or kill the process
# Stop UI: Ctrl+C
```

## Troubleshooting

### No logs appearing in ClickHouse
1. Check OTel collector is running: `docker logs ducklake-otel-clickhouse`
2. Verify backend is using OTel agent (check startup logs for "opentelemetry")
3. Check AUDIT marker is being set (logs should contain `[AUDIT]`)

### Jaeger shows no traces
1. Verify Jaeger is running: `curl http://localhost:16686`
2. Check OTel collector config exports to Jaeger
3. Restart OTel collector: `docker restart ducklake-otel-clickhouse`

### ClickHouse connection refused
1. Check ClickHouse is healthy: `docker exec ducklake-clickhouse clickhouse-client --query "SELECT 1"`
2. Verify port 8123 is not blocked

## Development

### Building

```bash
# Backend
cd service/be
./gradlew build

# UI
cd service/ui
npm run build
```

### Running Tests

```bash
cd service/be
./gradlew test
```
