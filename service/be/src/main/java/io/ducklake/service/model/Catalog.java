package io.ducklake.service.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.time.Instant;
import java.util.Map;

@Entity
@Table(name = "catalogs")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Catalog {

    @Id
    @Column(name = "catalog_id", length = 255)
    private String catalogId;

    @Column(name = "display_name", length = 255)
    private String displayName;

    @Column(name = "description", length = 1000)
    private String description;

    @Column(name = "metadata_uri", nullable = false, length = 1000)
    private String metadataUri;

    @Column(name = "data_path", nullable = false, length = 1000)
    private String dataPath;

    @Column(name = "metadata_type", length = 50)
    @Enumerated(EnumType.STRING)
    private MetadataType metadataType;

    @Column(name = "storage_type", length = 50)
    @Enumerated(EnumType.STRING)
    private StorageType storageType;

    @Column(name = "catalog_type", length = 50)
    @Enumerated(EnumType.STRING)
    @Builder.Default
    private CatalogType catalogType = CatalogType.DUCKLAKE;

    // Databricks-specific fields
    @Column(name = "databricks_workspace_url", length = 500)
    private String databricksWorkspaceUrl;

    @Column(name = "databricks_client_id", length = 255)
    private String databricksClientId;

    @Column(name = "databricks_client_secret", length = 500)
    private String databricksClientSecret;

    @Column(name = "databricks_catalog_name", length = 255)
    private String databricksCatalogName;

    // Snowflake-specific fields
    @Column(name = "snowflake_account", length = 255)
    private String snowflakeAccount;

    @Column(name = "snowflake_user", length = 255)
    private String snowflakeUser;

    @Column(name = "snowflake_password", length = 500)
    private String snowflakePassword;

    @Column(name = "snowflake_database", length = 255)
    private String snowflakeDatabase;

    @Column(name = "snowflake_warehouse", length = 255)
    private String snowflakeWarehouse;

    @Column(name = "enabled")
    @Builder.Default
    private Boolean enabled = true;

    @Column(name = "secret_name", length = 255)
    private String secretName;

    @Column(name = "options", columnDefinition = "TEXT")
    private String options; // JSON string

    @Column(name = "tags", columnDefinition = "TEXT")
    private String tags; // JSON array string

    @Column(name = "created_at")
    private Instant createdAt;

    @Column(name = "created_by", length = 255)
    private String createdBy;

    @Column(name = "updated_at")
    private Instant updatedAt;

    @Column(name = "last_accessed_at")
    private Instant lastAccessedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = Instant.now();
        updatedAt = Instant.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now();
    }

    public enum MetadataType {
        postgres, duckdb, sqlite
    }

    public enum StorageType {
        local, s3, gcs, azure
    }

    public enum CatalogType {
        DUCKLAKE,    // Native DuckLake with branching support
        DATABRICKS,  // Databricks Unity Catalog (read-only)
        SNOWFLAKE    // Snowflake (read-only)
    }
}
