package io.ducklake.service.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BranchDiffResponse {
    private String baseBranch;
    private Long baseSnapshotId;
    private String compareBranch;
    private Long compareSnapshotId;
    private List<SchemaDiff> schemas;
    private DiffSummary summary;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DiffSummary {
        private int schemasAdded;
        private int schemasRemoved;
        private int schemasModified;
        private int tablesAdded;
        private int tablesRemoved;
        private int tablesModified;
        private int columnsAdded;
        private int columnsRemoved;
        private int columnsModified;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SchemaDiff {
        private String schemaName;
        private String status; // added, removed, modified, unchanged
        private List<TableDiff> tables;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TableDiff {
        private String tableName;
        private String schemaName;
        private String status; // added, removed, modified, unchanged
        private Long baseRowCount;
        private Long compareRowCount;
        private List<ColumnDiff> columns;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ColumnDiff {
        private String columnName;
        private String status; // added, removed, modified, unchanged
        private String baseType;
        private String compareType;
        private Boolean baseNullable;
        private Boolean compareNullable;
    }
}
