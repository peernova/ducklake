package io.ducklake.service.event.model;

import lombok.Builder;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Generic resource reference - can be a table, dashboard, report, query, etc.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Resource {

    /**
     * Resource type: table, dashboard, report, query, branch, catalog, etc.
     */
    private String type;

    /**
     * Unique identifier for the resource.
     * Examples:
     * - table: "catalog.schema.table"
     * - dashboard: "dash_123"
     * - report: "rpt_456"
     */
    private String id;

    /**
     * Human-readable name for display and search.
     */
    private String name;

    /**
     * Flexible context/metadata as key-value pairs.
     * Examples:
     * - table: {catalog: "demo", schema: "trading", table: "positions", branch: "main"}
     * - dashboard: {dashboard_id: "dash_123", folder: "risk"}
     */
    private Map<String, String> path;
}
