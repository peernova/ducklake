package io.ducklake.service.controller;

import io.ducklake.service.exception.BranchException;
import io.ducklake.service.model.dto.*;
import io.ducklake.service.service.BranchService;
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
import java.util.Map;

@RestController
@RequestMapping("/api/v1/catalogs/{catalogId}/branches")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Branches", description = "Branch management for DuckLake catalogs")
public class BranchController {

    private final BranchService branchService;
    private final DiscoveryService discoveryService;

    @GetMapping
    @Operation(summary = "List branches", description = "List all branches for a catalog")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Branches retrieved successfully"),
            @ApiResponse(responseCode = "404", description = "Catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<List<BranchInfo>> listBranches(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId) {
        try {
            List<BranchInfo> branches = branchService.listBranches(catalogId);
            return ResponseEntity.ok(branches);
        } catch (SQLException e) {
            log.error("Failed to list branches for catalog: {}", catalogId, e);
            throw new BranchException("Failed to list branches: " + e.getMessage(), e);
        }
    }

    @GetMapping("/{branchName}")
    @Operation(summary = "Get branch", description = "Get branch details by name")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Branch found"),
            @ApiResponse(responseCode = "404", description = "Branch or catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<BranchInfo> getBranch(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId,
            @Parameter(description = "Branch name") @PathVariable String branchName) {
        try {
            return branchService.getBranch(catalogId, branchName)
                    .map(ResponseEntity::ok)
                    .orElse(ResponseEntity.notFound().build());
        } catch (SQLException e) {
            log.error("Failed to get branch {} from catalog {}", branchName, catalogId, e);
            throw new BranchException("Failed to get branch: " + e.getMessage(), e);
        }
    }

    @PostMapping
    @Operation(summary = "Create branch", description = "Create a new branch from an existing branch")
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Branch created successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid request - validation error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "404", description = "Parent branch or catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "409", description = "Branch with this name already exists",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<BranchInfo> createBranch(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId,
            @Valid @RequestBody BranchRequest request) {
        try {
            log.info("Creating branch {} in catalog {} from {}", request.getBranchName(), catalogId, request.getFromBranch());
            BranchInfo branch = branchService.createBranch(catalogId, request);
            return ResponseEntity.status(HttpStatus.CREATED).body(branch);
        } catch (SQLException e) {
            log.error("Failed to create branch {} in catalog {}", request.getBranchName(), catalogId, e);
            throw new BranchException("Failed to create branch: " + e.getMessage(), e);
        }
    }

    @DeleteMapping("/{branchName}")
    @Operation(summary = "Delete branch", description = "Delete a branch (soft delete)")
    @ApiResponses({
            @ApiResponse(responseCode = "204", description = "Branch deleted successfully"),
            @ApiResponse(responseCode = "400", description = "Cannot delete main branch",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "404", description = "Branch or catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<Void> deleteBranch(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId,
            @Parameter(description = "Branch name") @PathVariable String branchName) {
        try {
            branchService.deleteBranch(catalogId, branchName);
            return ResponseEntity.noContent().build();
        } catch (SQLException e) {
            log.error("Failed to delete branch {} from catalog {}", branchName, catalogId, e);
            throw new BranchException("Failed to delete branch: " + e.getMessage(), e);
        }
    }

    @GetMapping("/{branchName}/stats")
    @Operation(summary = "Get branch stats", description = "Get statistics for a branch including table count, file count, size")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Branch statistics retrieved successfully"),
            @ApiResponse(responseCode = "404", description = "Branch or catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<BranchStats> getBranchStats(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId,
            @Parameter(description = "Branch name") @PathVariable String branchName) {
        try {
            BranchStats stats = branchService.getBranchStats(catalogId, branchName);
            return ResponseEntity.ok(stats);
        } catch (SQLException e) {
            log.error("Failed to get stats for branch {} in catalog {}", branchName, catalogId, e);
            throw new BranchException("Failed to get branch stats: " + e.getMessage(), e);
        }
    }

    @GetMapping("/{branchName}/lineage")
    @Operation(summary = "Get branch lineage", description = "Get the parent chain for a branch")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Branch lineage retrieved successfully"),
            @ApiResponse(responseCode = "404", description = "Branch or catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<List<BranchInfo>> getBranchLineage(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId,
            @Parameter(description = "Branch name") @PathVariable String branchName) {
        try {
            List<BranchInfo> lineage = branchService.getBranchLineage(catalogId, branchName);
            return ResponseEntity.ok(lineage);
        } catch (SQLException e) {
            log.error("Failed to get lineage for branch {} in catalog {}", branchName, catalogId, e);
            throw new BranchException("Failed to get branch lineage: " + e.getMessage(), e);
        }
    }

    @GetMapping(":count")
    @Operation(summary = "Count branches", description = "Count active branches in a catalog")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Branch count retrieved successfully"),
            @ApiResponse(responseCode = "404", description = "Catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<Map<String, Long>> countBranches(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId) {
        try {
            long count = branchService.countBranches(catalogId);
            return ResponseEntity.ok(Map.of("count", count));
        } catch (SQLException e) {
            log.error("Failed to count branches for catalog {}", catalogId, e);
            throw new BranchException("Failed to count branches: " + e.getMessage(), e);
        }
    }

    @PostMapping(":search")
    @Operation(summary = "Search branches", description = "Search branches with flexible filtering")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Search completed successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid search parameters",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "404", description = "Catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<List<BranchInfo>> searchBranches(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId,
            @RequestBody SearchBranchesRequest request) {
        try {
            List<BranchInfo> branches = discoveryService.searchBranches(catalogId, request);
            return ResponseEntity.ok(branches);
        } catch (SQLException e) {
            log.error("Failed to search branches in catalog {}", catalogId, e);
            throw new BranchException("Failed to search branches: " + e.getMessage(), e);
        }
    }

    @GetMapping(":activity")
    @Operation(summary = "List branches by activity", description = "List branches sorted by activity")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Branch activity retrieved successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid orderBy parameter",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "404", description = "Catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<List<BranchInfo>> getBranchActivity(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId,
            @Parameter(description = "Sort field: last_modified, created_at, snapshot_count") @RequestParam(defaultValue = "last_modified") String orderBy,
            @Parameter(description = "Maximum number of branches to return") @RequestParam(defaultValue = "10") int limit) {
        try {
            List<BranchInfo> branches = discoveryService.getBranchActivity(catalogId, orderBy, limit);
            return ResponseEntity.ok(branches);
        } catch (SQLException e) {
            log.error("Failed to get branch activity for catalog {}", catalogId, e);
            throw new BranchException("Failed to get branch activity: " + e.getMessage(), e);
        }
    }

    @GetMapping(":by-age")
    @Operation(summary = "List branches by age", description = "List branches filtered by age in days")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Branches retrieved successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid days or status parameter",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "404", description = "Catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<List<BranchInfo>> getBranchesByAge(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId,
            @Parameter(description = "Number of days for age filter") @RequestParam int days,
            @Parameter(description = "Branch status filter: active, merged, archived") @RequestParam(required = false) String status) {
        try {
            List<BranchInfo> branches = discoveryService.getBranchesByAge(catalogId, days, status);
            return ResponseEntity.ok(branches);
        } catch (SQLException e) {
            log.error("Failed to get branches by age for catalog {}", catalogId, e);
            throw new BranchException("Failed to get branches by age: " + e.getMessage(), e);
        }
    }

    @PostMapping("/{branchName}:use")
    @Operation(summary = "Switch to branch", description = "Set the working branch for write operations")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Branch switched successfully"),
            @ApiResponse(responseCode = "404", description = "Branch or catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<BranchInfo> useBranch(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId,
            @Parameter(description = "Branch name to switch to") @PathVariable String branchName) {
        try {
            BranchInfo info = discoveryService.useBranch(catalogId, branchName);
            return ResponseEntity.ok(info);
        } catch (SQLException e) {
            log.error("Failed to switch to branch {} in catalog {}", branchName, catalogId, e);
            throw new BranchException("Failed to switch branch: " + e.getMessage(), e);
        }
    }

    @GetMapping("/diff")
    @Operation(summary = "Compare branches", description = "Get schema differences between two branches")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Diff computed successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid branch names or same branch specified for both",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "404", description = "Branch or catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<BranchDiffResponse> diffBranches(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId,
            @Parameter(description = "Base branch name for comparison") @RequestParam("base_branch") String baseBranch,
            @Parameter(description = "Branch to compare against base") @RequestParam("compare_branch") String compareBranch) {
        try {
            log.info("Computing diff between {} and {} in catalog {}", baseBranch, compareBranch, catalogId);
            BranchDiffResponse diff = discoveryService.diffBranches(catalogId, baseBranch, compareBranch);
            return ResponseEntity.ok(diff);
        } catch (SQLException e) {
            log.error("Failed to compute diff between {} and {} in catalog {}", baseBranch, compareBranch, catalogId, e);
            throw new BranchException("Failed to compute branch diff: " + e.getMessage(), e);
        }
    }

    @GetMapping("/changes")
    @Operation(summary = "Get branch changes", description = "Get snapshot changes since common ancestor between two branches")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Changes retrieved successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid branch names",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "404", description = "Branch or catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<BranchChangesResponse> getBranchChanges(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId,
            @Parameter(description = "Base branch name") @RequestParam("base_branch") String baseBranch,
            @Parameter(description = "Branch to compare") @RequestParam("compare_branch") String compareBranch,
            @Parameter(description = "Maximum number of changes per branch") @RequestParam(defaultValue = "50") int limit,
            @Parameter(description = "Offset for pagination") @RequestParam(defaultValue = "0") int offset) {
        try {
            log.info("Getting changes between {} and {} in catalog {}", baseBranch, compareBranch, catalogId);
            BranchChangesResponse changes = branchService.getBranchChanges(catalogId, baseBranch, compareBranch, limit, offset);
            return ResponseEntity.ok(changes);
        } catch (SQLException e) {
            log.error("Failed to get changes between {} and {} in catalog {}", baseBranch, compareBranch, catalogId, e);
            throw new BranchException("Failed to get branch changes: " + e.getMessage(), e);
        }
    }

    @GetMapping("/changes:count")
    @Operation(summary = "Count branch changes", description = "Get count of snapshot changes since common ancestor between two branches")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Count retrieved successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid branch names",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "404", description = "Branch or catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<BranchChangesResponse> getBranchChangesCount(
            @Parameter(description = "Catalog identifier") @PathVariable String catalogId,
            @Parameter(description = "Base branch name") @RequestParam("base_branch") String baseBranch,
            @Parameter(description = "Branch to compare") @RequestParam("compare_branch") String compareBranch) {
        try {
            log.info("Counting changes between {} and {} in catalog {}", baseBranch, compareBranch, catalogId);
            BranchChangesResponse changes = branchService.getBranchChangesCount(catalogId, baseBranch, compareBranch);
            return ResponseEntity.ok(changes);
        } catch (SQLException e) {
            log.error("Failed to count changes between {} and {} in catalog {}", baseBranch, compareBranch, catalogId, e);
            throw new BranchException("Failed to count branch changes: " + e.getMessage(), e);
        }
    }
}
