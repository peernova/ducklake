package io.ducklake.service.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "ducklake.duckdb")
@Data
public class DuckDBConfig {
    private String extensionPath = "../../../build/release/extension/ducklake/ducklake.duckdb_extension";
    private String postgresScannerPath;
    private int poolSize = 10;
    private long connectionTimeoutMs = 5000;
    private long queryTimeoutMs = 30000;
    private String tempDirectory = "/tmp/ducklake";
    private int threads = 4;
    private String memoryLimit = "4GB";
}
