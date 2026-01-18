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
public class SchemaInfo {
    private Long schemaId;
    private String schemaName;
    private String path;
    private Integer tableCount;
    private Integer viewCount;
    private Instant createdAt;
}
