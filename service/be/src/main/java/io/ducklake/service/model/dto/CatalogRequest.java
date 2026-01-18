package io.ducklake.service.model.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.Data;

@Data
public class CatalogRequest {
    @NotBlank(message = "Catalog ID is required")
    @Pattern(regexp = "^[a-zA-Z][a-zA-Z0-9_]*$", message = "Catalog ID must start with a letter and contain only alphanumeric characters and underscores")
    private String catalogId;

    private String displayName;
    private String description;

    // Catalog type: DUCKLAKE, DATABRICKS, SNOWFLAKE
    private String catalogType = "DUCKLAKE";

    // DuckLake-specific fields (required for DUCKLAKE type)
    private String metadataUri;
    private String dataPath;
    private String metadataType = "postgres";
    private String storageType = "local";
    private String secretName;

    // Databricks-specific fields (required for DATABRICKS type)
    private String databricksWorkspaceUrl;
    private String databricksClientId;
    private String databricksClientSecret;
    private String databricksCatalogName;

    // Snowflake-specific fields (required for SNOWFLAKE type)
    private String snowflakeAccount;
    private String snowflakeUser;
    private String snowflakePassword;
    private String snowflakeDatabase;
    private String snowflakeWarehouse;
}
