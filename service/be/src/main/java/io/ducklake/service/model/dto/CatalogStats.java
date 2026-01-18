package io.ducklake.service.model.dto;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CatalogStats {
    private String catalogName;
    private Long branchCount;
    private Long activeBranchCount;
    private Long schemaCount;
    private Long tableCount;
    private Long viewCount;
    private Long dataFileCount;
    private Long deleteFileCount;
    private Long totalRows;
    private Long totalSizeBytes;
    private Long snapshotCount;
}
