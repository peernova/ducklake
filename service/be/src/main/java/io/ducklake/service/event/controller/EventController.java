package io.ducklake.service.event.controller;

import io.ducklake.service.event.model.AccessEvent;
import io.ducklake.service.event.model.Favorite;
import io.ducklake.service.event.model.Resource;
import io.ducklake.service.event.model.SearchQuery;
import io.ducklake.service.event.service.EventService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * REST API for querying access events and managing favorites.
 *
 * NOTE: Event WRITES are handled by OpenTelemetry, not this API.
 * This controller only handles READS (queries) and favorites CRUD.
 */
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Tag(name = "Events", description = "Access events query and favorites management")
public class EventController {

    private final EventService eventService;

    // ========== Read Events ==========

    @GetMapping("/users/{userId}/recent")
    @Operation(summary = "Get recent activity", description = "Get recently accessed resources for a user")
    public ResponseEntity<List<AccessEvent>> getRecentActivity(
            @Parameter(description = "User ID") @PathVariable String userId,
            @Parameter(description = "Filter by resource type") @RequestParam(required = false) String type,
            @Parameter(description = "Maximum results") @RequestParam(defaultValue = "20") int limit) {

        List<AccessEvent> events = type != null
                ? eventService.getRecentActivity(userId, type, limit)
                : eventService.getRecentActivity(userId, limit);

        return ResponseEntity.ok(events);
    }

    @PostMapping("/events/search")
    @Operation(summary = "Search events", description = "Search access events with filters")
    public ResponseEntity<List<AccessEvent>> searchEvents(@RequestBody SearchRequest request) {
        SearchQuery query = SearchQuery.builder()
                .text(request.getText())
                .userId(request.getUserId())
                .resourceTypes(request.getResourceTypes())
                .operations(request.getOperations())
                .statuses(request.getStatuses())
                .fromTime(request.getFromTime())
                .toTime(request.getToTime())
                .limit(request.getLimit() != null ? request.getLimit() : 50)
                .offset(request.getOffset() != null ? request.getOffset() : 0)
                .build();

        List<AccessEvent> events = eventService.search(query);
        return ResponseEntity.ok(events);
    }

    @GetMapping("/audit/{resourceType}/{resourceId}")
    @Operation(summary = "Get audit trail", description = "Get audit trail for a specific resource")
    public ResponseEntity<List<AccessEvent>> getAuditTrail(
            @Parameter(description = "Resource type") @PathVariable String resourceType,
            @Parameter(description = "Resource ID") @PathVariable String resourceId,
            @Parameter(description = "Maximum results") @RequestParam(defaultValue = "100") int limit) {

        List<AccessEvent> events = eventService.getAuditTrail(resourceType, resourceId, limit);
        return ResponseEntity.ok(events);
    }

    // ========== Favorites ==========

    @GetMapping("/users/{userId}/favorites")
    @Operation(summary = "List favorites", description = "List all favorites for a user")
    public ResponseEntity<List<Favorite>> listFavorites(
            @Parameter(description = "User ID") @PathVariable String userId,
            @Parameter(description = "Filter by resource type") @RequestParam(required = false) String type) {

        List<Favorite> favorites = type != null
                ? eventService.listFavorites(userId, type)
                : eventService.listFavorites(userId);

        return ResponseEntity.ok(favorites);
    }

    @PostMapping("/users/{userId}/favorites")
    @Operation(summary = "Add favorite", description = "Add a resource to favorites")
    public ResponseEntity<Favorite> addFavorite(
            @Parameter(description = "User ID") @PathVariable String userId,
            @RequestBody FavoriteRequest request) {

        Favorite favorite = eventService.addFavorite(userId, request.getResource(), request.getDisplayName());
        return ResponseEntity.status(HttpStatus.CREATED).body(favorite);
    }

    @DeleteMapping("/users/{userId}/favorites/{resourceType}/{resourceId}")
    @Operation(summary = "Remove favorite", description = "Remove a resource from favorites")
    public ResponseEntity<Void> removeFavorite(
            @Parameter(description = "User ID") @PathVariable String userId,
            @Parameter(description = "Resource type") @PathVariable String resourceType,
            @Parameter(description = "Resource ID") @PathVariable String resourceId) {

        boolean removed = eventService.removeFavorite(userId, resourceType, resourceId);
        return removed ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    @GetMapping("/users/{userId}/favorites/{resourceType}/{resourceId}")
    @Operation(summary = "Check if favorited", description = "Check if a resource is favorited")
    public ResponseEntity<Map<String, Boolean>> isFavorited(
            @Parameter(description = "User ID") @PathVariable String userId,
            @Parameter(description = "Resource type") @PathVariable String resourceType,
            @Parameter(description = "Resource ID") @PathVariable String resourceId) {

        boolean favorited = eventService.isFavorited(userId, resourceType, resourceId);
        return ResponseEntity.ok(Map.of("favorited", favorited));
    }

    // ========== Request DTOs ==========

    @Data
    public static class SearchRequest {
        private String text;
        private String userId;
        private List<String> resourceTypes;
        private List<String> operations;
        private List<String> statuses;
        private Instant fromTime;
        private Instant toTime;
        private Integer limit;
        private Integer offset;
    }

    @Data
    public static class FavoriteRequest {
        private Resource resource;
        private String displayName;
    }
}
