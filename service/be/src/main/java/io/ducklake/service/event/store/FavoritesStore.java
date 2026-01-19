package io.ducklake.service.event.store;

import io.ducklake.service.event.model.Favorite;
import io.ducklake.service.event.model.Resource;

import java.util.List;
import java.util.Optional;

/**
 * Interface for managing user favorites.
 *
 * Implementations can target different backends:
 * - PostgreSQL (simple, ACID)
 * - ClickHouse (using ReplacingMergeTree)
 */
public interface FavoritesStore {

    /**
     * Add a resource to user's favorites.
     *
     * @param userId the user ID
     * @param resource the resource to favorite
     * @param displayName optional custom display name
     * @return the created favorite
     */
    Favorite add(String userId, Resource resource, String displayName);

    /**
     * Remove a resource from user's favorites.
     *
     * @param userId the user ID
     * @param resourceType the resource type
     * @param resourceId the resource ID
     * @return true if removed, false if not found
     */
    boolean remove(String userId, String resourceType, String resourceId);

    /**
     * List all favorites for a user.
     *
     * @param userId the user ID
     * @return list of favorites ordered by creation time (newest first)
     */
    List<Favorite> list(String userId);

    /**
     * List favorites for a user filtered by resource type.
     *
     * @param userId the user ID
     * @param resourceType filter by resource type
     * @return list of favorites
     */
    List<Favorite> listByType(String userId, String resourceType);

    /**
     * Check if a resource is favorited by a user.
     *
     * @param userId the user ID
     * @param resourceType the resource type
     * @param resourceId the resource ID
     * @return the favorite if exists
     */
    Optional<Favorite> get(String userId, String resourceType, String resourceId);

    /**
     * Check if a resource is favorited by a user.
     *
     * @param userId the user ID
     * @param resourceType the resource type
     * @param resourceId the resource ID
     * @return true if favorited
     */
    boolean isFavorited(String userId, String resourceType, String resourceId);

    /**
     * Initialize the store (create tables, indexes, etc.).
     * Called on application startup.
     */
    void initialize();
}
