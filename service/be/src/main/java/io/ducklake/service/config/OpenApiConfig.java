package io.ducklake.service.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenApiConfig {

    @Value("${spring.application.name:DuckLake Service}")
    private String applicationName;

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title(applicationName + " API")
                        .version("1.0.0")
                        .description("""
                                DuckLake Service API provides a REST interface for managing DuckLake catalogs with branching support.

                                ## Features
                                - **Catalog Management**: Register, update, and delete DuckLake catalogs
                                - **Branch Management**: Create, list, and manage branches for data versioning
                                - **Schema Discovery**: List schemas, tables, and columns with branch awareness
                                - **Query Execution**: Execute SQL queries with branch context
                                - **Branch Diff**: Compare schemas between branches

                                ## Error Handling
                                All errors follow gRPC-style status conventions with the following structure:
                                - `code`: gRPC status code (0-16)
                                - `status`: Error code name (e.g., INVALID_ARGUMENT, NOT_FOUND)
                                - `message`: Developer-facing error message
                                - `details`: Additional error details (field violations, debug info)
                                - `traceId`: OpenTelemetry trace ID for distributed tracing
                                - `spanId`: OpenTelemetry span ID

                                ## Common Error Codes
                                | Code | Status | HTTP | Description |
                                |------|--------|------|-------------|
                                | 3 | INVALID_ARGUMENT | 400 | Invalid request parameters |
                                | 5 | NOT_FOUND | 404 | Resource not found |
                                | 6 | ALREADY_EXISTS | 409 | Resource already exists |
                                | 7 | PERMISSION_DENIED | 403 | Insufficient permissions |
                                | 9 | FAILED_PRECONDITION | 400 | Precondition not met |
                                | 4 | DEADLINE_EXCEEDED | 504 | Request timeout |
                                | 13 | INTERNAL | 500 | Internal server error |
                                """)
                        .contact(new Contact()
                                .name("DuckLake Team")
                                .email("support@ducklake.io"))
                        .license(new License()
                                .name("MIT License")
                                .url("https://opensource.org/licenses/MIT")))
                .servers(List.of(
                        new Server().url("/").description("Default server")))
                .components(new Components()
                        .addResponses("BadRequest", createErrorResponse("Invalid request - validation error or malformed input"))
                        .addResponses("NotFound", createErrorResponse("Resource not found"))
                        .addResponses("Conflict", createErrorResponse("Resource already exists or conflict"))
                        .addResponses("Forbidden", createErrorResponse("Permission denied"))
                        .addResponses("Timeout", createErrorResponse("Request timeout - operation took too long"))
                        .addResponses("InternalError", createErrorResponse("Internal server error")));
    }

    private ApiResponse createErrorResponse(String description) {
        return new ApiResponse()
                .description(description)
                .content(new Content()
                        .addMediaType("application/json",
                                new MediaType().schema(new Schema<>().$ref("#/components/schemas/ErrorResponse"))));
    }
}
