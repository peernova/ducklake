package io.ducklake.service.service;

import io.ducklake.service.exception.CatalogException;
import io.ducklake.service.model.Catalog;
import io.ducklake.service.model.dto.CatalogRequest;
import io.ducklake.service.repository.CatalogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

@Service
@Slf4j
@RequiredArgsConstructor
public class CatalogService {

    private static final Pattern CATALOG_ID_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]*$");
    private static final int MAX_CATALOG_ID_LENGTH = 255;

    private final CatalogRepository catalogRepository;

    public List<Catalog> listCatalogs() {
        return catalogRepository.findByEnabledTrue();
    }

    public List<Catalog> searchCatalogs(String search) {
        if (search == null || search.isEmpty()) {
            return listCatalogs();
        }
        return catalogRepository.search(search);
    }

    public Optional<Catalog> getCatalog(String catalogId) {
        return catalogRepository.findByCatalogIdAndEnabledTrue(catalogId);
    }

    @Transactional
    public Catalog createCatalog(CatalogRequest request) {
        validateCatalogId(request.getCatalogId());

        if (catalogRepository.existsById(request.getCatalogId())) {
            throw CatalogException.alreadyExists(request.getCatalogId());
        }

        Catalog.CatalogType catalogType = Catalog.CatalogType.valueOf(
                request.getCatalogType() != null ? request.getCatalogType() : "DUCKLAKE");

        // Validate required fields based on catalog type
        validateCatalogTypeFields(request, catalogType);

        Catalog.CatalogBuilder builder = Catalog.builder()
                .catalogId(request.getCatalogId())
                .displayName(request.getDisplayName())
                .description(request.getDescription())
                .catalogType(catalogType)
                .enabled(true);

        // Set type-specific fields
        switch (catalogType) {
            case DUCKLAKE:
                String metadataUri = request.getMetadataUri();
                Catalog.MetadataType metadataType = Catalog.MetadataType.valueOf(
                        request.getMetadataType() != null ? request.getMetadataType() : "postgres");

                // Auto-generate postgres metadata URI if not provided
                if ((metadataUri == null || metadataUri.isBlank()) && metadataType == Catalog.MetadataType.postgres) {
                    metadataUri = String.format("postgres:dbname=%s host=localhost port=5433 user=postgres password=postgres",
                            request.getCatalogId());
                    // Create the postgres database
                    createPostgresDatabase(request.getCatalogId());
                }

                builder.metadataUri(metadataUri)
                       .dataPath(request.getDataPath())
                       .metadataType(metadataType)
                       .storageType(Catalog.StorageType.valueOf(
                               request.getStorageType() != null ? request.getStorageType() : "local"))
                       .secretName(request.getSecretName());
                break;
            case DATABRICKS:
                builder.databricksWorkspaceUrl(request.getDatabricksWorkspaceUrl())
                       .databricksClientId(request.getDatabricksClientId())
                       .databricksClientSecret(request.getDatabricksClientSecret())
                       .databricksCatalogName(request.getDatabricksCatalogName());
                break;
            case SNOWFLAKE:
                builder.snowflakeAccount(request.getSnowflakeAccount())
                       .snowflakeUser(request.getSnowflakeUser())
                       .snowflakePassword(request.getSnowflakePassword())
                       .snowflakeDatabase(request.getSnowflakeDatabase())
                       .snowflakeWarehouse(request.getSnowflakeWarehouse());
                break;
        }

        Catalog catalog = builder.build();
        log.info("Creating {} catalog: {}", catalogType, catalog.getCatalogId());
        return catalogRepository.save(catalog);
    }

    private void validateCatalogTypeFields(CatalogRequest request, Catalog.CatalogType type) {
        switch (type) {
            case DUCKLAKE:
                // metadataUri is optional for postgres type - will be auto-generated
                String metadataType = request.getMetadataType() != null ? request.getMetadataType() : "postgres";
                if (!"postgres".equals(metadataType) && (request.getMetadataUri() == null || request.getMetadataUri().isBlank())) {
                    throw CatalogException.invalidArgument("metadataUri", "Metadata URI is required for non-postgres DuckLake catalogs");
                }
                if (request.getDataPath() == null || request.getDataPath().isBlank()) {
                    throw CatalogException.invalidArgument("dataPath", "Data path is required for DuckLake catalogs");
                }
                break;
            case DATABRICKS:
                if (request.getDatabricksWorkspaceUrl() == null || request.getDatabricksWorkspaceUrl().isBlank()) {
                    throw CatalogException.invalidArgument("databricksWorkspaceUrl", "Workspace URL is required for Databricks catalogs");
                }
                if (request.getDatabricksClientId() == null || request.getDatabricksClientId().isBlank()) {
                    throw CatalogException.invalidArgument("databricksClientId", "Client ID is required for Databricks catalogs");
                }
                if (request.getDatabricksClientSecret() == null || request.getDatabricksClientSecret().isBlank()) {
                    throw CatalogException.invalidArgument("databricksClientSecret", "Client secret is required for Databricks catalogs");
                }
                if (request.getDatabricksCatalogName() == null || request.getDatabricksCatalogName().isBlank()) {
                    throw CatalogException.invalidArgument("databricksCatalogName", "Catalog name is required for Databricks catalogs");
                }
                break;
            case SNOWFLAKE:
                if (request.getSnowflakeAccount() == null || request.getSnowflakeAccount().isBlank()) {
                    throw CatalogException.invalidArgument("snowflakeAccount", "Account is required for Snowflake catalogs");
                }
                if (request.getSnowflakeUser() == null || request.getSnowflakeUser().isBlank()) {
                    throw CatalogException.invalidArgument("snowflakeUser", "User is required for Snowflake catalogs");
                }
                if (request.getSnowflakeDatabase() == null || request.getSnowflakeDatabase().isBlank()) {
                    throw CatalogException.invalidArgument("snowflakeDatabase", "Database is required for Snowflake catalogs");
                }
                break;
        }
    }

    @Transactional
    public Optional<Catalog> updateCatalog(String catalogId, CatalogRequest request) {
        return catalogRepository.findById(catalogId).map(existing -> {
            if (request.getDisplayName() != null) {
                existing.setDisplayName(request.getDisplayName());
            }
            if (request.getDescription() != null) {
                existing.setDescription(request.getDescription());
            }
            if (request.getMetadataUri() != null) {
                existing.setMetadataUri(request.getMetadataUri());
            }
            if (request.getDataPath() != null) {
                existing.setDataPath(request.getDataPath());
            }
            if (request.getSecretName() != null) {
                existing.setSecretName(request.getSecretName());
            }
            log.info("Updating catalog: {}", catalogId);
            return catalogRepository.save(existing);
        });
    }

    @Transactional
    public boolean deleteCatalog(String catalogId) {
        return catalogRepository.findById(catalogId).map(catalog -> {
            catalog.setEnabled(false);
            catalogRepository.save(catalog);
            log.info("Disabled catalog: {}", catalogId);
            return true;
        }).orElse(false);
    }

    @Transactional
    public void updateLastAccessed(String catalogId) {
        catalogRepository.findById(catalogId).ifPresent(catalog -> {
            catalog.setLastAccessedAt(Instant.now());
            catalogRepository.save(catalog);
        });
    }

    private void validateCatalogId(String catalogId) {
        if (catalogId == null || catalogId.isBlank()) {
            throw CatalogException.invalidCatalogId(catalogId, "Catalog ID is required");
        }
        if (!CATALOG_ID_PATTERN.matcher(catalogId).matches()) {
            throw CatalogException.invalidCatalogId(catalogId,
                    "Catalog ID must start with a letter and contain only letters, numbers, and underscores. " +
                    "Invalid characters or format in: '" + catalogId + "'");
        }
        if (catalogId.length() > MAX_CATALOG_ID_LENGTH) {
            throw CatalogException.invalidCatalogId(catalogId,
                    "Catalog ID must be " + MAX_CATALOG_ID_LENGTH + " characters or less. Got: " + catalogId.length());
        }
    }

    private void createPostgresDatabase(String dbName) {
        // Connect to postgres database to create new database
        String url = "jdbc:postgresql://localhost:5433/postgres";
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, "postgres", "postgres");
             java.sql.Statement stmt = conn.createStatement()) {
            // Check if database exists
            java.sql.ResultSet rs = stmt.executeQuery(
                    "SELECT 1 FROM pg_database WHERE datname = '" + dbName + "'");
            if (!rs.next()) {
                stmt.execute("CREATE DATABASE " + dbName);
                log.info("Created postgres database: {}", dbName);
            } else {
                log.debug("Database {} already exists", dbName);
            }
        } catch (java.sql.SQLException e) {
            throw new RuntimeException("Failed to create postgres database: " + dbName, e);
        }
    }
}
