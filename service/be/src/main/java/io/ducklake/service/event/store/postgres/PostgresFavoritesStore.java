package io.ducklake.service.event.store.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ducklake.service.event.model.Favorite;
import io.ducklake.service.event.model.Resource;
import io.ducklake.service.event.store.FavoritesStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * PostgreSQL implementation of FavoritesStore.
 */
@Slf4j
@RequiredArgsConstructor
public class PostgresFavoritesStore implements FavoritesStore {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    private static final String CREATE_TABLE_SQL = """
        CREATE TABLE IF NOT EXISTS user_favorites (
            id VARCHAR(36) PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            resource_type VARCHAR(50) NOT NULL,
            resource_id VARCHAR(500) NOT NULL,
            resource_name VARCHAR(500),
            resource_path JSONB,
            display_name VARCHAR(500),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

            UNIQUE(user_id, resource_type, resource_id)
        )
        """;

    private static final String CREATE_INDEXES_SQL = """
        CREATE INDEX IF NOT EXISTS idx_favorites_user ON user_favorites(user_id);
        CREATE INDEX IF NOT EXISTS idx_favorites_user_type ON user_favorites(user_id, resource_type);
        """;

    @Override
    public void initialize() {
        log.info("Initializing PostgreSQL favorites store...");
        jdbcTemplate.execute(CREATE_TABLE_SQL);
        for (String sql : CREATE_INDEXES_SQL.split(";")) {
            if (!sql.trim().isEmpty()) {
                jdbcTemplate.execute(sql.trim());
            }
        }
        log.info("PostgreSQL favorites store initialized");
    }

    @Override
    public Favorite add(String userId, Resource resource, String displayName) {
        String id = UUID.randomUUID().toString();
        Instant now = Instant.now();

        String sql = """
            INSERT INTO user_favorites (id, user_id, resource_type, resource_id, resource_name, resource_path, display_name, created_at)
            VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?)
            ON CONFLICT (user_id, resource_type, resource_id) DO UPDATE SET
                resource_name = EXCLUDED.resource_name,
                resource_path = EXCLUDED.resource_path,
                display_name = EXCLUDED.display_name
            RETURNING id, created_at
            """;

        // Use queryForObject to get the returned id and created_at
        try {
            jdbcTemplate.update(sql,
                    id,
                    userId,
                    resource.getType(),
                    resource.getId(),
                    resource.getName(),
                    toJson(resource.getPath()),
                    displayName != null ? displayName : resource.getName(),
                    Timestamp.from(now)
            );
        } catch (Exception e) {
            log.warn("Failed to add favorite, might already exist: {}", e.getMessage());
        }

        return Favorite.builder()
                .id(id)
                .userId(userId)
                .resource(resource)
                .displayName(displayName != null ? displayName : resource.getName())
                .createdAt(now)
                .build();
    }

    @Override
    public boolean remove(String userId, String resourceType, String resourceId) {
        String sql = "DELETE FROM user_favorites WHERE user_id = ? AND resource_type = ? AND resource_id = ?";
        int rows = jdbcTemplate.update(sql, userId, resourceType, resourceId);
        return rows > 0;
    }

    @Override
    public List<Favorite> list(String userId) {
        String sql = """
            SELECT id, user_id, resource_type, resource_id, resource_name, resource_path, display_name, created_at
            FROM user_favorites
            WHERE user_id = ?
            ORDER BY created_at DESC
            """;
        return jdbcTemplate.query(sql, new FavoriteRowMapper(), userId);
    }

    @Override
    public List<Favorite> listByType(String userId, String resourceType) {
        String sql = """
            SELECT id, user_id, resource_type, resource_id, resource_name, resource_path, display_name, created_at
            FROM user_favorites
            WHERE user_id = ? AND resource_type = ?
            ORDER BY created_at DESC
            """;
        return jdbcTemplate.query(sql, new FavoriteRowMapper(), userId, resourceType);
    }

    @Override
    public Optional<Favorite> get(String userId, String resourceType, String resourceId) {
        String sql = """
            SELECT id, user_id, resource_type, resource_id, resource_name, resource_path, display_name, created_at
            FROM user_favorites
            WHERE user_id = ? AND resource_type = ? AND resource_id = ?
            """;
        List<Favorite> results = jdbcTemplate.query(sql, new FavoriteRowMapper(), userId, resourceType, resourceId);
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    @Override
    public boolean isFavorited(String userId, String resourceType, String resourceId) {
        String sql = "SELECT COUNT(*) FROM user_favorites WHERE user_id = ? AND resource_type = ? AND resource_id = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, userId, resourceType, resourceId);
        return count != null && count > 0;
    }

    private String toJson(Map<String, String> map) {
        if (map == null) return null;
        try {
            return objectMapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize resource path: {}", e.getMessage());
            return "{}";
        }
    }

    private Map<String, String> fromJson(String json) {
        if (json == null || json.isBlank()) return null;
        try {
            return objectMapper.readValue(json, new TypeReference<>() {});
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize resource path: {}", e.getMessage());
            return null;
        }
    }

    private class FavoriteRowMapper implements RowMapper<Favorite> {
        @Override
        public Favorite mapRow(ResultSet rs, int rowNum) throws SQLException {
            Resource resource = Resource.builder()
                    .type(rs.getString("resource_type"))
                    .id(rs.getString("resource_id"))
                    .name(rs.getString("resource_name"))
                    .path(fromJson(rs.getString("resource_path")))
                    .build();

            Timestamp ts = rs.getTimestamp("created_at");

            return Favorite.builder()
                    .id(rs.getString("id"))
                    .userId(rs.getString("user_id"))
                    .resource(resource)
                    .displayName(rs.getString("display_name"))
                    .createdAt(ts != null ? ts.toInstant() : null)
                    .build();
        }
    }
}
