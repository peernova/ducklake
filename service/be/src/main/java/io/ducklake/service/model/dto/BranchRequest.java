package io.ducklake.service.model.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.Data;

@Data
public class BranchRequest {
    @NotBlank(message = "Branch name is required")
    @Pattern(regexp = "^[a-zA-Z][a-zA-Z0-9_-]*$", message = "Branch name must start with a letter and contain only alphanumeric characters, underscores, and hyphens")
    private String branchName;

    private String fromBranch = "main";
    private Long fromSnapshot;
}
