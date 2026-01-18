package io.ducklake.service;

import io.ducklake.service.model.dto.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class QueryControllerTest extends BaseIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    // ==================== Success Cases ====================

    @Test
    @Order(1)
    void executeQuery_simpleSelect_success() {
        QueryRequest request = new QueryRequest();
        request.setSql("SELECT 1 as num, 'hello' as greeting");

        ResponseEntity<QueryResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/query",
                request,
                QueryResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getRowCount()).isEqualTo(1L);
        assertThat(response.getBody().getColumns()).hasSize(2);
    }

    @Test
    @Order(2)
    void executeQuery_arithmetic_success() {
        QueryRequest request = new QueryRequest();
        request.setSql("SELECT 10 + 20 as result");

        ResponseEntity<QueryResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/query",
                request,
                QueryResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getRows()).isNotEmpty();
    }

    @Test
    @Order(3)
    void executeQuery_multipleRows_success() {
        QueryRequest request = new QueryRequest();
        request.setSql("SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)");

        ResponseEntity<QueryResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/query",
                request,
                QueryResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getRowCount()).isEqualTo(3L);
    }

    @Test
    @Order(4)
    void executeQuery_aggregation_success() {
        QueryRequest request = new QueryRequest();
        request.setSql("SELECT COUNT(*) as cnt, SUM(i) as total FROM generate_series(1, 10) AS t(i)");

        ResponseEntity<QueryResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/query",
                request,
                QueryResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
    }

    @Test
    @Order(5)
    void executeQuery_duckdbFunction_success() {
        QueryRequest request = new QueryRequest();
        request.setSql("SELECT current_date() as today, version() as duckdb_version");

        ResponseEntity<QueryResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/query",
                request,
                QueryResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
    }

    @Test
    @Order(6)
    void executeQuery_responseHasExecutionTime() {
        QueryRequest request = new QueryRequest();
        request.setSql("SELECT * FROM generate_series(1, 100)");

        ResponseEntity<QueryResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/query",
                request,
                QueryResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getExecutionTimeMs()).isNotNull();
        assertThat(response.getBody().getExecutionTimeMs()).isGreaterThanOrEqualTo(0);
    }

    @Test
    @Order(7)
    void executeQuery_responseHasCorrectColumnInfo() {
        QueryRequest request = new QueryRequest();
        request.setSql("SELECT 1::INTEGER as int_col, 'text'::VARCHAR as str_col, 3.14::DOUBLE as dbl_col");

        ResponseEntity<QueryResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/query",
                request,
                QueryResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getColumns()).hasSize(3);

        var columns = response.getBody().getColumns();
        assertThat(columns.get(0).getName()).isEqualTo("int_col");
        assertThat(columns.get(1).getName()).isEqualTo("str_col");
        assertThat(columns.get(2).getName()).isEqualTo("dbl_col");
    }

    // ==================== Error Cases ====================

    @Test
    @Order(20)
    void executeQuery_syntaxError() {
        QueryRequest request = new QueryRequest();
        request.setSql("SELEC * FROM table");

        ResponseEntity<ErrorResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/query",
                request,
                ErrorResponse.class);

        assertThat(response.getStatusCode()).isIn(HttpStatus.BAD_REQUEST, HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isNotNull();
    }

    @Test
    @Order(21)
    void executeQuery_missingTable() {
        QueryRequest request = new QueryRequest();
        request.setSql("SELECT * FROM nonexistent_table_xyz");

        ResponseEntity<ErrorResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/query",
                request,
                ErrorResponse.class);

        assertThat(response.getStatusCode()).isIn(HttpStatus.NOT_FOUND, HttpStatus.BAD_REQUEST, HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isNotNull();
    }

    @Test
    @Order(22)
    void executeQuery_missingSql() {
        QueryRequest request = new QueryRequest();

        ResponseEntity<ErrorResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/query",
                request,
                ErrorResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getStatus()).isEqualTo("INVALID_ARGUMENT");
    }

    @Test
    @Order(23)
    void executeQuery_emptySql() {
        QueryRequest request = new QueryRequest();
        request.setSql("");

        ResponseEntity<ErrorResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/query",
                request,
                ErrorResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getStatus()).isEqualTo("INVALID_ARGUMENT");
    }

    @Test
    @Order(24)
    void executeQuery_blankSql() {
        QueryRequest request = new QueryRequest();
        request.setSql("   ");

        ResponseEntity<ErrorResponse> response = restTemplate.postForEntity(
                baseUrl() + "/api/v1/query",
                request,
                ErrorResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
