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
public class SearchBranchesRequest {
    private String pattern;
    private String status;
    private Instant createdAfter;
    private Instant createdBefore;
    private String parentBranch;
}
