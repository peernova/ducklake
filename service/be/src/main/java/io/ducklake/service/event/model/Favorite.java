package io.ducklake.service.event.model;

import lombok.Builder;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents a user's favorited resource.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Favorite {

    /**
     * Unique favorite identifier.
     */
    private String id;

    /**
     * User who favorited the resource.
     */
    private String userId;

    /**
     * The favorited resource.
     */
    private Resource resource;

    /**
     * User-defined display name (can override resource name).
     */
    private String displayName;

    /**
     * When the favorite was created.
     */
    private Instant createdAt;
}
