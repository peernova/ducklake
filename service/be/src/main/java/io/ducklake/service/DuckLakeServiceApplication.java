package io.ducklake.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class DuckLakeServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DuckLakeServiceApplication.class, args);
    }
}
