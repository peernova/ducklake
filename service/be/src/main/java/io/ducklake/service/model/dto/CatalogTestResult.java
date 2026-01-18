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
public class CatalogTestResult {
    private String catalogId;
    private String overallStatus; // healthy, degraded, unhealthy
    private MetadataBackendStatus metadataBackend;
    private DataStorageStatus dataStorage;
    private Integer branchCount;
    private Integer tableCount;
    private Instant testedAt;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MetadataBackendStatus {
        private String status; // connected, error
        private Double latencyMs;
        private String version;
        private String error;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DataStorageStatus {
        private String status; // accessible, error
        private Double latencyMs;
        private String path;
        private String error;
    }
}
