package io.ducklake.service.model.dto;

import lombok.Data;
import jakarta.validation.constraints.NotBlank;
import java.util.Map;

@Data
public class QueryRequest {
    @NotBlank(message = "SQL query is required")
    private String sql;

    private Map<String, String> branchContext;
    private Map<String, Long> snapshotContext;
    private Map<String, Object> parameters;
    private Long timeoutMs = 30000L;
}
