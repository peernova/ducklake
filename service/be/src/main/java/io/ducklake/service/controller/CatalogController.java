package io.ducklake.service.controller;

import io.ducklake.service.model.Catalog;
import io.ducklake.service.model.dto.*;
import io.ducklake.service.service.CatalogService;
import io.ducklake.service.service.DiscoveryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.List;

@RestController
@RequestMapping("/api/v1/catalogs")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Catalogs", description = "Catalog registry management")
public class CatalogController {

    private final CatalogService catalogService;
    private final DiscoveryService discoveryService;

    @GetMapping
    @Operation(summary = "List catalogs", description = "List all enabled catalogs with optional search filter")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Catalogs retrieved successfully"),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<List<Catalog>> listCatalogs(
            @Parameter(description = "Optional search filter for catalog name or description")
            @RequestParam(required = false) String search) {
        List<Catalog> catalogs = catalogService.searchCatalogs(search);
        return ResponseEntity.ok(catalogs);
    }

    @GetMapping("/{catalogId}")
    @Operation(summary = "Get catalog", description = "Get a specific catalog by ID")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Catalog found"),
            @ApiResponse(responseCode = "404", description = "Catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<Catalog> getCatalog(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId) {
        return catalogService.getCatalog(catalogId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping
    @Operation(summary = "Create catalog", description = "Register a new DuckLake catalog")
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Catalog created successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid request - validation error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "409", description = "Catalog already exists",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<Catalog> createCatalog(@Valid @RequestBody CatalogRequest request) {
        log.info("Creating catalog: {}", request.getCatalogId());
        Catalog catalog = catalogService.createCatalog(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(catalog);
    }

    @PutMapping("/{catalogId}")
    @Operation(summary = "Update catalog", description = "Update an existing catalog")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Catalog updated successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid request - validation error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "404", description = "Catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<Catalog> updateCatalog(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId,
            @Valid @RequestBody CatalogRequest request) {
        return catalogService.updateCatalog(catalogId, request)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{catalogId}")
    @Operation(summary = "Delete catalog", description = "Disable a catalog (soft delete)")
    @ApiResponses({
            @ApiResponse(responseCode = "204", description = "Catalog deleted successfully"),
            @ApiResponse(responseCode = "404", description = "Catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<Void> deleteCatalog(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId) {
        if (catalogService.deleteCatalog(catalogId)) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.notFound().build();
    }

    @PostMapping("/{catalogId}:test")
    @Operation(summary = "Test catalog connection", description = "Test connectivity to catalog's metadata and storage")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Connection test completed"),
            @ApiResponse(responseCode = "404", description = "Catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "504", description = "Connection timeout",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<CatalogTestResult> testCatalog(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId) throws SQLException {
        CatalogTestResult result = discoveryService.testCatalog(catalogId);
        return ResponseEntity.ok(result);
    }

    @GetMapping("/{catalogId}/stats")
    @Operation(summary = "Get catalog stats", description = "Get catalog-wide statistics (DuckLake catalogs only)")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Statistics retrieved successfully"),
            @ApiResponse(responseCode = "404", description = "Catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "400", description = "Catalog is not a DuckLake catalog",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<CatalogStats> getCatalogStats(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId) throws SQLException {
        CatalogStats stats = discoveryService.getCatalogStats(catalogId);
        return ResponseEntity.ok(stats);
    }

    @GetMapping("/{catalogId}/schemas")
    @Operation(summary = "List schemas", description = "List all schemas in the catalog")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Schemas retrieved successfully"),
            @ApiResponse(responseCode = "404", description = "Catalog or branch not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<List<SchemaInfo>> listSchemas(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId,
            @Parameter(description = "Branch name (optional, defaults to main)") @RequestParam(required = false) String branch) throws SQLException {
        List<SchemaInfo> schemas = discoveryService.listSchemas(catalogId, branch);
        return ResponseEntity.ok(schemas);
    }

    @GetMapping("/{catalogId}/schemas/{schemaName}/tables")
    @Operation(summary = "List tables", description = "List all tables in a schema")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Tables retrieved successfully"),
            @ApiResponse(responseCode = "404", description = "Catalog, schema, or branch not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<List<TableInfo>> listTables(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId,
            @Parameter(description = "Schema name") @PathVariable String schemaName,
            @Parameter(description = "Branch name (optional, defaults to main)") @RequestParam(required = false) String branch) throws SQLException {
        List<TableInfo> tables = discoveryService.listTables(catalogId, schemaName, branch);
        return ResponseEntity.ok(tables);
    }

    @GetMapping("/{catalogId}/schemas/{schemaName}/tables/{tableName}")
    @Operation(summary = "Get table info", description = "Get detailed information about a table")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Table info retrieved successfully"),
            @ApiResponse(responseCode = "404", description = "Table not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<TableInfo> getTableInfo(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId,
            @Parameter(description = "Schema name") @PathVariable String schemaName,
            @Parameter(description = "Table name") @PathVariable String tableName,
            @Parameter(description = "Branch name (optional, defaults to main)") @RequestParam(required = false) String branch) throws SQLException {
        TableInfo info = discoveryService.getTableInfo(catalogId, schemaName, tableName, branch);
        if (info == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(info);
    }

    @GetMapping("/{catalogId}/current-branch")
    @Operation(summary = "Get current branch", description = "Get the current working branch")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Current branch retrieved successfully"),
            @ApiResponse(responseCode = "404", description = "Catalog not found or no current branch set",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<BranchInfo> getCurrentBranch(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId) throws SQLException {
        BranchInfo info = discoveryService.getCurrentBranch(catalogId);
        if (info == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(info);
    }
}
