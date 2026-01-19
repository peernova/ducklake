package io.ducklake.service.event.store.clickhouse;

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
 * ClickHouse implementation of FavoritesStore.
 * Uses ReplacingMergeTree for upsert behavior.
 */
@Slf4j
@RequiredArgsConstructor
public class ClickHouseFavoritesStore implements FavoritesStore {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    private static final String CREATE_TABLE_SQL = """
        CREATE TABLE IF NOT EXISTS user_favorites (
            user_id String,
            resource_type LowCardinality(String),
            resource_id String,
            resource_name String,
            resource_path String,
            display_name String,
            created_at DateTime64(3),
            deleted UInt8 DEFAULT 0
        )
        ENGINE = ReplacingMergeTree(created_at)
        ORDER BY (user_id, resource_type, resource_id)
        """;

    @Override
    public void initialize() {
        log.info("Initializing ClickHouse favorites store...");
        jdbcTemplate.execute(CREATE_TABLE_SQL);
        log.info("ClickHouse favorites store initialized");
    }

    @Override
    public Favorite add(String userId, Resource resource, String displayName) {
        Instant now = Instant.now();

        String sql = """
            INSERT INTO user_favorites (
                user_id, resource_type, resource_id, resource_name, resource_path,
                display_name, created_at, deleted
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0)
            """;

        jdbcTemplate.update(sql,
                userId,
                resource.getType(),
                resource.getId(),
                resource.getName() != null ? resource.getName() : "",
                toJson(resource.getPath()),
                displayName != null ? displayName : (resource.getName() != null ? resource.getName() : ""),
                Timestamp.from(now)
        );

        return Favorite.builder()
                .id(UUID.randomUUID().toString()) // ClickHouse doesn't have auto-generated IDs
                .userId(userId)
                .resource(resource)
                .displayName(displayName != null ? displayName : resource.getName())
                .createdAt(now)
                .build();
    }

    @Override
    public boolean remove(String userId, String resourceType, String resourceId) {
        // In ReplacingMergeTree, we insert a "tombstone" with deleted=1
        // The next merge will remove the row
        String sql = """
            INSERT INTO user_favorites (
                user_id, resource_type, resource_id, resource_name, resource_path,
                display_name, created_at, deleted
            ) VALUES (?, ?, ?, '', '{}', '', now64(), 1)
            """;

        jdbcTemplate.update(sql, userId, resourceType, resourceId);
        return true; // ClickHouse doesn't tell us if row existed
    }

    @Override
    public List<Favorite> list(String userId) {
        // FINAL forces merge to get latest version
        String sql = """
            SELECT user_id, resource_type, resource_id, resource_name, resource_path,
                   display_name, created_at
            FROM user_favorites FINAL
            WHERE user_id = ? AND deleted = 0
            ORDER BY created_at DESC
            """;
        return jdbcTemplate.query(sql, new FavoriteRowMapper(), userId);
    }

    @Override
    public List<Favorite> listByType(String userId, String resourceType) {
        String sql = """
            SELECT user_id, resource_type, resource_id, resource_name, resource_path,
                   display_name, created_at
            FROM user_favorites FINAL
            WHERE user_id = ? AND resource_type = ? AND deleted = 0
            ORDER BY created_at DESC
            """;
        return jdbcTemplate.query(sql, new FavoriteRowMapper(), userId, resourceType);
    }

    @Override
    public Optional<Favorite> get(String userId, String resourceType, String resourceId) {
        String sql = """
            SELECT user_id, resource_type, resource_id, resource_name, resource_path,
                   display_name, created_at
            FROM user_favorites FINAL
            WHERE user_id = ? AND resource_type = ? AND resource_id = ? AND deleted = 0
            """;
        List<Favorite> results = jdbcTemplate.query(sql, new FavoriteRowMapper(),
                userId, resourceType, resourceId);
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    @Override
    public boolean isFavorited(String userId, String resourceType, String resourceId) {
        String sql = """
            SELECT count() FROM user_favorites FINAL
            WHERE user_id = ? AND resource_type = ? AND resource_id = ? AND deleted = 0
            """;
        Long count = jdbcTemplate.queryForObject(sql, Long.class, userId, resourceType, resourceId);
        return count != null && count > 0;
    }

    private String toJson(Map<String, String> map) {
        if (map == null) return "{}";
        try {
            return objectMapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize resource path: {}", e.getMessage());
            return "{}";
        }
    }

    private Map<String, String> fromJson(String json) {
        if (json == null || json.isBlank() || "{}".equals(json)) return null;
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
                    .name(emptyToNull(rs.getString("resource_name")))
                    .path(fromJson(rs.getString("resource_path")))
                    .build();

            Timestamp ts = rs.getTimestamp("created_at");

            return Favorite.builder()
                    .id(resource.getType() + ":" + resource.getId()) // Composite ID
                    .userId(rs.getString("user_id"))
                    .resource(resource)
                    .displayName(emptyToNull(rs.getString("display_name")))
                    .createdAt(ts != null ? ts.toInstant() : null)
                    .build();
        }

        private String emptyToNull(String s) {
            return (s == null || s.isEmpty()) ? null : s;
        }
    }
}
