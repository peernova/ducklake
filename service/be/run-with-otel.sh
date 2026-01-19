#!/bin/bash
#
# Run DuckLake Service with OTel Java Agent
#
# The agent auto-instruments:
# - Traces (HTTP, JDBC, etc.)
# - Logs (SLF4J with MDC)
# - Metrics
#
# Download agent: https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AGENT_PATH="${OTEL_AGENT_PATH:-$SCRIPT_DIR/opentelemetry-javaagent.jar}"

# Download agent if not present
if [ ! -f "$AGENT_PATH" ]; then
    echo "Downloading OTel Java Agent..."
    curl -L -o "$AGENT_PATH" \
        https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/latest/download/opentelemetry-javaagent.jar
fi

# Default config - override with env vars
export OTEL_SERVICE_NAME="${OTEL_SERVICE_NAME:-ducklake-service}"
export OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-http://localhost:4317}"

# Traces → Jaeger, Logs → Collector
export OTEL_TRACES_EXPORTER="${OTEL_TRACES_EXPORTER:-otlp}"
export OTEL_LOGS_EXPORTER="${OTEL_LOGS_EXPORTER:-otlp}"
export OTEL_METRICS_EXPORTER="${OTEL_METRICS_EXPORTER:-otlp}"

# Capture MDC as log attributes
export OTEL_INSTRUMENTATION_LOGBACK_MDC_ENABLED=true

echo "Starting DuckLake Service with OTel Agent..."
echo "  Service: $OTEL_SERVICE_NAME"
echo "  Endpoint: $OTEL_EXPORTER_OTLP_ENDPOINT"

java -javaagent:"$AGENT_PATH" \
     -jar "$SCRIPT_DIR/build/libs/be-0.0.1-SNAPSHOT.jar" \
     "$@"
