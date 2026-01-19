#!/usr/bin/env python3
"""
PostgreSQL Loader for OTel AUDIT Logs

Reads OTLP JSON file exported by OTel Collector and inserts into PostgreSQL.

Usage:
    python pg_loader.py [--watch]           # One-time load or continuous watch
    python pg_loader.py --file audit.json   # Load specific file

Environment variables:
    POSTGRES_HOST     (default: localhost)
    POSTGRES_PORT     (default: 5434)
    POSTGRES_USER     (default: ducklake)
    POSTGRES_PASSWORD (default: ducklake)
    POSTGRES_DB       (default: ducklake_service)
    AUDIT_LOG_FILE    (default: /data/audit_logs.json)
"""

import json
import os
import sys
import time
import argparse
from datetime import datetime
from pathlib import Path

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("Install psycopg2: pip install psycopg2-binary")
    sys.exit(1)


def get_db_connection():
    """Get PostgreSQL connection from environment."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5434")),
        user=os.getenv("POSTGRES_USER", "ducklake"),
        password=os.getenv("POSTGRES_PASSWORD", "ducklake"),
        database=os.getenv("POSTGRES_DB", "ducklake_service"),
    )


def parse_otlp_attributes(attributes):
    """Convert OTLP attributes list to dict."""
    result = {}
    for attr in attributes:
        key = attr.get("key", "")
        value = attr.get("value", {})
        # OTLP values have type wrappers: stringValue, intValue, etc.
        if "stringValue" in value:
            result[key] = value["stringValue"]
        elif "intValue" in value:
            result[key] = int(value["intValue"])
        elif "doubleValue" in value:
            result[key] = float(value["doubleValue"])
        elif "boolValue" in value:
            result[key] = value["boolValue"]
        elif "arrayValue" in value:
            result[key] = [
                v.get("stringValue", str(v)) for v in value["arrayValue"].get("values", [])
            ]
        else:
            result[key] = str(value)
    return result


def extract_log_record(log_record, resource_attrs, scope_attrs):
    """Extract access event from OTLP log record."""
    attrs = parse_otlp_attributes(log_record.get("attributes", []))

    # Merge resource and scope attributes (lower priority)
    all_attrs = {**resource_attrs, **scope_attrs, **attrs}

    # Parse timestamp (nanoseconds to datetime)
    ts_nanos = int(log_record.get("timeUnixNano", 0))
    if ts_nanos:
        timestamp = datetime.fromtimestamp(ts_nanos / 1e9)
    else:
        timestamp = datetime.now()

    # Extract trace context
    trace_id = log_record.get("traceId", "") or all_attrs.get("trace_id", "")
    span_id = log_record.get("spanId", "") or all_attrs.get("span_id", "")

    # Build raw_data with full context for future improvements
    raw_data = {
        "log_record": log_record,
        "resource_attributes": resource_attrs,
        "scope_attributes": scope_attrs,
        "parsed_attributes": all_attrs,
    }

    # Build record for PostgreSQL
    return {
        "event_id": all_attrs.get("event_id", ""),
        "timestamp": timestamp,
        "trace_id": trace_id,
        "span_id": span_id,
        "user_id": all_attrs.get("user_id", "unknown"),
        "user_email": all_attrs.get("user_email", ""),
        "source_ip": all_attrs.get("source_ip", ""),
        "client_info": all_attrs.get("client_info", ""),
        "resource_type": all_attrs.get("resource_type", ""),
        "resource_id": all_attrs.get("resource_id", ""),
        "resource_name": all_attrs.get("resource_name", ""),
        "resource_path": json.dumps(all_attrs.get("resource_path", {})) if all_attrs.get("resource_path") else "{}",
        "operation": all_attrs.get("operation", ""),
        "status": all_attrs.get("status", ""),
        "rejection_reason": all_attrs.get("rejection_reason", ""),
        "rows_affected": int(all_attrs.get("rows_affected", 0) or 0),
        "execution_time_ms": int(all_attrs.get("execution_time_ms", 0) or 0),
        "raw_data": json.dumps(raw_data),
    }


def parse_otlp_json(data):
    """Parse OTLP JSON export format and extract log records."""
    records = []

    # Handle both single object and newline-delimited JSON
    if isinstance(data, dict):
        data_list = [data]
    elif isinstance(data, list):
        data_list = data
    else:
        return records

    for item in data_list:
        resource_logs = item.get("resourceLogs", [])
        for rl in resource_logs:
            resource_attrs = parse_otlp_attributes(
                rl.get("resource", {}).get("attributes", [])
            )

            for sl in rl.get("scopeLogs", []):
                scope_attrs = parse_otlp_attributes(
                    sl.get("scope", {}).get("attributes", [])
                )

                for log_record in sl.get("logRecords", []):
                    record = extract_log_record(log_record, resource_attrs, scope_attrs)
                    if record["event_id"]:  # Only include valid events
                        records.append(record)

    return records


def load_file(filepath):
    """Load and parse OTLP JSON file (supports newline-delimited)."""
    records = []
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                records.extend(parse_otlp_json(data))
            except json.JSONDecodeError as e:
                print(f"Warning: Invalid JSON line: {e}")
    return records


def insert_records(conn, records):
    """Batch insert records into PostgreSQL."""
    if not records:
        return 0

    columns = [
        "event_id", "timestamp", "trace_id", "span_id",
        "user_id", "user_email", "source_ip", "client_info",
        "resource_type", "resource_id", "resource_name", "resource_path",
        "operation", "status", "rejection_reason", "rows_affected", "execution_time_ms",
        "raw_data"
    ]

    sql = f"""
        INSERT INTO access_log ({', '.join(columns)})
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    # Note: ON CONFLICT requires a unique constraint on event_id
    # For now, we'll just insert (duplicates may occur on re-runs)
    sql = f"""
        INSERT INTO access_log ({', '.join(columns)})
        VALUES %s
    """

    values = [
        tuple(r[col] for col in columns)
        for r in records
    ]

    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()

    return len(records)


def watch_file(filepath, interval=5):
    """Watch file for changes and load new records."""
    print(f"Watching {filepath} for changes (interval: {interval}s)...")
    last_size = 0
    last_mtime = 0

    conn = get_db_connection()
    print("Connected to PostgreSQL")

    try:
        while True:
            path = Path(filepath)
            if path.exists():
                stat = path.stat()
                if stat.st_size != last_size or stat.st_mtime != last_mtime:
                    print(f"File changed, loading...")
                    records = load_file(filepath)
                    if records:
                        count = insert_records(conn, records)
                        print(f"Inserted {count} records")
                    last_size = stat.st_size
                    last_mtime = stat.st_mtime
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopped")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Load OTel AUDIT logs into PostgreSQL")
    parser.add_argument("--file", "-f", help="OTLP JSON file to load")
    parser.add_argument("--watch", "-w", action="store_true", help="Watch file for changes")
    parser.add_argument("--interval", "-i", type=int, default=5, help="Watch interval in seconds")
    args = parser.parse_args()

    filepath = args.file or os.getenv("AUDIT_LOG_FILE", "/data/audit_logs.json")

    if args.watch:
        watch_file(filepath, args.interval)
    else:
        # One-time load
        if not Path(filepath).exists():
            print(f"File not found: {filepath}")
            sys.exit(1)

        records = load_file(filepath)
        print(f"Parsed {len(records)} records from {filepath}")

        if records:
            conn = get_db_connection()
            count = insert_records(conn, records)
            conn.close()
            print(f"Inserted {count} records into PostgreSQL")
        else:
            print("No valid AUDIT records found")


if __name__ == "__main__":
    main()
