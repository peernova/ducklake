package io.ducklake.service.model.dto;

import lombok.Data;
import jakarta.validation.constraints.NotBlank;

/**
 * Request for executing DDL/DML statements on a specific branch.
 * This ensures branch context is set atomically with the statement execution.
 */
@Data
public class ExecuteRequest {
    @NotBlank(message = "SQL statement is required")
    private String sql;

    @NotBlank(message = "Branch name is required")
    private String branchName;

    private Long timeoutMs = 30000L;
}
