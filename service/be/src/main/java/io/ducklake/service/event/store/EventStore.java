package io.ducklake.service.event.store;

import io.ducklake.service.event.model.AccessEvent;
import io.ducklake.service.event.model.SearchQuery;

import java.util.List;

/**
 * Read-only interface for querying access events.
 *
 * WRITES: Use OpenTelemetry (EventEmitter) - battle-tested async, batching, reliability
 * READS:  Use this interface - query ClickHouse/PostgreSQL directly
 *
 * Why this design:
 * - OTel SDK handles buffering, batching, retries, backpressure (don't reinvent)
 * - OTel Collector routes to any backend (ClickHouse, Kafka, S3, etc.)
 * - We only build what's unique: query API for recent/search/audit
 */
public interface EventStore {

    /**
     * Get recent activity for a user (for "recently accessed" sidebar).
     * Returns distinct resources ordered by most recent access.
     */
    List<AccessEvent> getRecentByUser(String userId, int limit);

    /**
     * Get recent activity for a user filtered by resource type.
     */
    List<AccessEvent> getRecentByUserAndType(String userId, String resourceType, int limit);

    /**
     * Search events with flexible filters.
     */
    List<AccessEvent> search(SearchQuery query);

    /**
     * Get audit trail for a specific resource.
     */
    List<AccessEvent> getAuditTrail(String resourceType, String resourceId, int limit);

    /**
     * Initialize the store (create tables if needed).
     * Tables may also be created by OTel Collector on first write.
     */
    void initialize();
}
