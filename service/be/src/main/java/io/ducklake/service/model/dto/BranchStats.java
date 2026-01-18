package io.ducklake.service.model.dto;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BranchStats {
    private String branchName;
    private Long tableCount;
    private Long schemaCount;
    private Long viewCount;
    private Long dataFileCount;
    private Long totalRows;
    private Long totalSizeBytes;
    private Long snapshotCount;
    private Instant lastModifiedAt;
    // TODO: Track last_accessed_at based on query logs in the service layer
    // DuckLake storage layer doesn't track SELECT queries
    private Instant lastAccessedAt;
}
