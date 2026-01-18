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
public class QueryResponse {
    private List<ColumnInfo> columns;
    private List<List<Object>> rows;
    private Long rowCount;
    private Double executionTimeMs;
    private String branch;
    private Long snapshotId;

    // Tracing info for audit
    private String traceId;
    private String spanId;
}
