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
public class QueryTableReference {
    private String catalogName;
    private String schemaName;
    private String tableName;
    private String branchName;
    private String referenceType;
    private List<String> columns;
}
