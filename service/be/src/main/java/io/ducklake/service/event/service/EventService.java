package io.ducklake.service.event.service;

import io.ducklake.service.event.emitter.EventEmitter;
import io.ducklake.service.event.model.AccessEvent;
import io.ducklake.service.event.model.Favorite;
import io.ducklake.service.event.model.Resource;
import io.ducklake.service.event.model.SearchQuery;
import io.ducklake.service.event.store.EventStore;
import io.ducklake.service.event.store.FavoritesStore;
import io.ducklake.service.model.dto.QueryTableReference;
import io.micrometer.observation.annotation.Observed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Service layer for access events and favorites.
 *
 * Architecture:
 * - WRITES: Via OpenTelemetry (EventEmitter) → OTel Collector → Store
 * - READS:  Via EventStore interface → ClickHouse/PostgreSQL
 *
 * Why this split:
 * - OTel handles async, batching, retries (don't reinvent)
 * - We focus on query logic (recent, search, audit)
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class EventService {

    private final EventEmitter eventEmitter;
    private final EventStore eventStore;
    private final FavoritesStore favoritesStore;

    @PostConstruct
    public void initialize() {
        eventStore.initialize();
        favoritesStore.initialize();
        log.info("EventService initialized - writes via OTel, reads via EventStore");
    }

    // ========== Write Events (via OTel) ==========

    /**
     * Record an access event. Non-blocking (~100ns).
     */
    public void recordEvent(AccessEvent event) {
        eventEmitter.emit(event);
    }

    /**
     * Record a view event.
     */
    public void recordView(String userId, Resource resource) {
        eventEmitter.emitView(userId, resource);
    }

    /**
     * Record a query execution.
     */
    public void recordQuery(String userId, Resource resource, int executionTimeMs, long rowsReturned) {
        eventEmitter.emitQuery(userId, resource, executionTimeMs, rowsReturned);
    }

    /**
     * Record a rejected access.
     */
    public void recordRejection(String userId, Resource resource, String operation, String reason) {
        eventEmitter.emitRejection(userId, resource, operation, reason);
    }

    /**
     * Record access to tables from a query execution.
     * Emits one event per table accessed.
     *
     * @param userId User who executed the query
     * @param tableReferences Tables accessed by the query
     * @param executionTimeMs Total query execution time
     * @param rowsReturned Rows returned by the query
     * @param traceId OTel trace ID for correlation
     * @param spanId OTel span ID for correlation
     */
    public void recordQueryTableAccess(String userId, List<QueryTableReference> tableReferences,
                                       int executionTimeMs, long rowsReturned,
                                       String traceId, String spanId) {
        if (tableReferences == null || tableReferences.isEmpty()) {
            return;
        }

        for (QueryTableReference ref : tableReferences) {
            // Build resource path with full context
            Map<String, String> path = new HashMap<>();
            if (ref.getCatalogName() != null) path.put("catalog", ref.getCatalogName());
            if (ref.getSchemaName() != null) path.put("schema", ref.getSchemaName());
            if (ref.getBranchName() != null) path.put("branch", ref.getBranchName());
            if (ref.getColumns() != null && !ref.getColumns().isEmpty()) {
                path.put("columns", String.join(",", ref.getColumns()));
            }

            // Build resource ID: catalog/schema/table
            String resourceId = String.join("/",
                    ref.getCatalogName() != null ? ref.getCatalogName() : "",
                    ref.getSchemaName() != null ? ref.getSchemaName() : "",
                    ref.getTableName() != null ? ref.getTableName() : "");

            Resource resource = Resource.builder()
                    .type("table")
                    .id(resourceId)
                    .name(ref.getTableName())
                    .path(path)
                    .build();

            // Map reference type to operation
            String operation = mapReferenceTypeToOperation(ref.getReferenceType());

            AccessEvent event = AccessEvent.builder()
                    .userId(userId)
                    .resource(resource)
                    .operation(operation)
                    .status("success")
                    .executionTimeMs(executionTimeMs)
                    .rowsAffected(rowsReturned)
                    .traceId(traceId)
                    .spanId(spanId)
                    .build();

            eventEmitter.emit(event);
        }
    }

    private String mapReferenceTypeToOperation(String referenceType) {
        if (referenceType == null) return "query";
        return switch (referenceType.toUpperCase()) {
            case "SELECT", "READ" -> "select";
            case "INSERT" -> "insert";
            case "UPDATE" -> "update";
            case "DELETE" -> "delete";
            case "CREATE" -> "create";
            case "DROP" -> "drop";
            case "ALTER" -> "alter";
            default -> "query";
        };
    }

    // ========== Read Events (via EventStore) ==========

    /**
     * Get recent activity for a user.
     */
    @Observed(name = "event.recent", contextualName = "get-recent-activity")
    public List<AccessEvent> getRecentActivity(String userId, int limit) {
        return eventStore.getRecentByUser(userId, limit);
    }

    /**
     * Get recent activity filtered by resource type.
     */
    public List<AccessEvent> getRecentActivity(String userId, String resourceType, int limit) {
        return eventStore.getRecentByUserAndType(userId, resourceType, limit);
    }

    /**
     * Search access events.
     */
    @Observed(name = "event.search", contextualName = "search-events")
    public List<AccessEvent> search(SearchQuery query) {
        return eventStore.search(query);
    }

    /**
     * Get audit trail for a resource.
     */
    @Observed(name = "event.audit", contextualName = "get-audit-trail")
    public List<AccessEvent> getAuditTrail(String resourceType, String resourceId, int limit) {
        return eventStore.getAuditTrail(resourceType, resourceId, limit);
    }

    // ========== Favorites ==========

    public Favorite addFavorite(String userId, Resource resource, String displayName) {
        return favoritesStore.add(userId, resource, displayName);
    }

    public boolean removeFavorite(String userId, String resourceType, String resourceId) {
        return favoritesStore.remove(userId, resourceType, resourceId);
    }

    public List<Favorite> listFavorites(String userId) {
        return favoritesStore.list(userId);
    }

    public List<Favorite> listFavorites(String userId, String resourceType) {
        return favoritesStore.listByType(userId, resourceType);
    }

    public boolean isFavorited(String userId, String resourceType, String resourceId) {
        return favoritesStore.isFavorited(userId, resourceType, resourceId);
    }

    public Optional<Favorite> getFavorite(String userId, String resourceType, String resourceId) {
        return favoritesStore.get(userId, resourceType, resourceId);
    }
}
