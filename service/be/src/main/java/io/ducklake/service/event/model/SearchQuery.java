package io.ducklake.service.event.model;

import lombok.Builder;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

/**
 * Search query for access events.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SearchQuery {

    /**
     * Free text search across resource names.
     */
    private String text;

    /**
     * Filter by user ID.
     */
    private String userId;

    /**
     * Filter by resource types.
     */
    private List<String> resourceTypes;

    /**
     * Filter by operations.
     */
    private List<String> operations;

    /**
     * Filter by status.
     */
    private List<String> statuses;

    /**
     * Start of time range.
     */
    private Instant fromTime;

    /**
     * End of time range.
     */
    private Instant toTime;

    /**
     * Maximum results to return.
     */
    @Builder.Default
    private int limit = 50;

    /**
     * Offset for pagination.
     */
    @Builder.Default
    private int offset = 0;
}
