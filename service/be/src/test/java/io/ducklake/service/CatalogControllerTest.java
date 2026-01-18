package io.ducklake.service;

import io.ducklake.service.model.Catalog;
import io.ducklake.service.model.dto.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CatalogControllerTest extends BaseIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    private static String testCatalogId;

    @BeforeAll
    static void setup() {
        testCatalogId = "catalog_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    }

    // ==================== Success Cases ====================

    @Test
    @Order(1)
    void createCatalog_success() {
        CatalogRequest request = new CatalogRequest();
        request.setCatalogId(testCatalogId);
        request.setMetadataUri("postgres:host=" + postgres.getHost() +
                " port=" + postgres.getFirstMappedPort() +
                " dbname=" + postgres.getDatabaseName() +
                " user=" + postgres.getUsername() +
                " password=" + postgres.getPassword());
        request.setDataPath("/tmp/ducklake_test_data/" + testCatalogId);

        ResponseEntity<Catalog> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/catalogs",
                request,
                Catalog.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getCatalogId()).isEqualTo(testCatalogId);
    }

    @Test
    @Order(2)
    void listCatalogs_success() {
        ResponseEntity<List<Catalog>> response = restTemplate.exchange(
                baseUrl() + "/api/v1/catalogs",
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<>() {});

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody()).anyMatch(c -> c.getCatalogId().equals(testCatalogId));
    }

    @Test
    @Order(3)
    void getCatalog_success() {
        ResponseEntity<Catalog> response = restTemplate.getForEntity(
                baseUrl() + "/api/v1/catalogs/" + testCatalogId,
                Catalog.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getCatalogId()).isEqualTo(testCatalogId);
    }

    @Test
    @Order(4)
    void listCatalogs_withSearch_success() {
        ResponseEntity<List<Catalog>> response = restTemplate.exchange(
                baseUrl() + "/api/v1/catalogs?search=catalog",
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<>() {});

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
    }

    @Test
    @Order(5)
    void updateCatalog_success() {
        CatalogRequest request = new CatalogRequest();
        request.setCatalogId(testCatalogId);
        request.setMetadataUri("postgres:host=" + postgres.getHost() +
                " port=" + postgres.getFirstMappedPort() +
                " dbname=" + postgres.getDatabaseName() +
                " user=" + postgres.getUsername() +
                " password=" + postgres.getPassword());
        request.setDataPath("/tmp/ducklake_test_data/" + testCatalogId + "_updated");

        HttpEntity<CatalogRequest> entity = new HttpEntity<>(request);
        ResponseEntity<Catalog> response = restTemplate.exchange(
                baseUrl() + "/api/v1/catalogs/" + testCatalogId,
                HttpMethod.PUT,
                entity,
                Catalog.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getDataPath()).contains("updated");
    }

    // ==================== Validation Error Cases ====================

    @Test
    @Order(10)
    void getCatalog_notFound() {
        ResponseEntity<ErrorResponse> response = restTemplate.getForEntity(
                baseUrl() + "/api/v1/catalogs/notfoundcatalog",
                ErrorResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @Order(11)
    void createCatalog_duplicateId() {
        CatalogRequest request = new CatalogRequest();
        request.setCatalogId(testCatalogId);
        request.setMetadataUri("postgres:host=localhost");
        request.setDataPath("/tmp/data");

        ResponseEntity<ErrorResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/catalogs",
                request,
                ErrorResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getStatus()).isEqualTo("ALREADY_EXISTS");
    }

    @Test
    @Order(12)
    void createCatalog_missingCatalogId() {
        CatalogRequest request = new CatalogRequest();
        request.setMetadataUri("postgres:host=localhost");
        request.setDataPath("/tmp/data");

        ResponseEntity<ErrorResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/catalogs",
                request,
                ErrorResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getStatus()).isEqualTo("INVALID_ARGUMENT");
    }

    @Test
    @Order(13)
    void createCatalog_missingMetadataUri() {
        CatalogRequest request = new CatalogRequest();
        request.setCatalogId("nometa_" + UUID.randomUUID().toString().substring(0, 8).replace("-", ""));
        request.setDataPath("/tmp/data");

        ResponseEntity<ErrorResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/catalogs",
                request,
                ErrorResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getStatus()).isEqualTo("INVALID_ARGUMENT");
    }

    @Test
    @Order(14)
    void createCatalog_invalidCatalogId_withDashes() {
        CatalogRequest request = new CatalogRequest();
        request.setCatalogId("invalid-catalog-id");
        request.setMetadataUri("postgres:host=localhost");
        request.setDataPath("/tmp/data");

        ResponseEntity<ErrorResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/catalogs",
                request,
                ErrorResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getStatus()).isEqualTo("INVALID_ARGUMENT");
        // Validation message comes from @Pattern annotation
        assertThat(response.getBody().getDetails()).isNotEmpty();
    }

    @Test
    @Order(15)
    void createCatalog_invalidCatalogId_startsWithNumber() {
        CatalogRequest request = new CatalogRequest();
        request.setCatalogId("123catalog");
        request.setMetadataUri("postgres:host=localhost");
        request.setDataPath("/tmp/data");

        ResponseEntity<ErrorResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/catalogs",
                request,
                ErrorResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getStatus()).isEqualTo("INVALID_ARGUMENT");
    }

    @Test
    @Order(16)
    void updateCatalog_notFound() {
        CatalogRequest request = new CatalogRequest();
        request.setCatalogId("notfound");
        request.setMetadataUri("postgres:host=localhost");
        request.setDataPath("/tmp/data");

        HttpEntity<CatalogRequest> entity = new HttpEntity<>(request);
        ResponseEntity<Catalog> response = restTemplate.exchange(
                baseUrl() + "/api/v1/catalogs/notfound",
                HttpMethod.PUT,
                entity,
                Catalog.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @Order(17)
    void deleteCatalog_notFound() {
        ResponseEntity<Void> response = restTemplate.exchange(
                baseUrl() + "/api/v1/catalogs/notfound",
                HttpMethod.DELETE,
                null,
                Void.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    // ==================== Cleanup ====================

    @Test
    @Order(99)
    void deleteCatalog_success() {
        ResponseEntity<Void> response = restTemplate.exchange(
                baseUrl() + "/api/v1/catalogs/" + testCatalogId,
                HttpMethod.DELETE,
                null,
                Void.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
    }
}
