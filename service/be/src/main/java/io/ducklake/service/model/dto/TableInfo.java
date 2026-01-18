package io.ducklake.service.model.dto;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableInfo {
    private String catalogName;
    private String schemaName;
    private String tableName;
    private String referenceType;
    private List<ColumnInfo> columns;
    private Long rowCount;
    private Long totalSizeBytes;
}
