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
public class BranchInfo {
    private Long branchId;
    private String branchName;
    private Long parentBranchId;
    private String parentBranchName;
    private Long forkSnapshotId;
    private Long headSnapshotId;
    private String status;
    private Instant createdAt;
    private String createdBy;
    private Boolean isActive;
}
