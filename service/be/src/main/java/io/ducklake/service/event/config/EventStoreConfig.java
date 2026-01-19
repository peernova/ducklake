package io.ducklake.service.event.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ducklake.service.event.store.EventStore;
import io.ducklake.service.event.store.FavoritesStore;
import io.ducklake.service.event.store.clickhouse.ClickHouseEventStore;
import io.ducklake.service.event.store.clickhouse.ClickHouseFavoritesStore;
import io.ducklake.service.event.store.postgres.PostgresEventStore;
import io.ducklake.service.event.store.postgres.PostgresFavoritesStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableAsync;

import javax.sql.DataSource;

/**
 * Configuration for event store backend selection.
 *
 * Set event.store.type=postgres or event.store.type=clickhouse in application.properties
 *
 * For PostgreSQL (default):
 *   event.store.type=postgres
 *   event.store.postgres.url=jdbc:postgresql://localhost:5432/ducklake
 *   event.store.postgres.username=postgres
 *   event.store.postgres.password=postgres
 *
 * For ClickHouse:
 *   event.store.type=clickhouse
 *   event.store.clickhouse.url=jdbc:clickhouse://localhost:8123/default
 *   event.store.clickhouse.username=default
 *   event.store.clickhouse.password=
 */
@Configuration
@EnableAsync
@Slf4j
public class EventStoreConfig {

    // ========== PostgreSQL Configuration ==========

    @Configuration
    @ConditionalOnProperty(name = "event.store.type", havingValue = "postgres", matchIfMissing = true)
    public static class PostgresConfig {

        @Value("${event.store.postgres.url:jdbc:postgresql://localhost:5432/ducklake}")
        private String url;

        @Value("${event.store.postgres.username:postgres}")
        private String username;

        @Value("${event.store.postgres.password:postgres}")
        private String password;

        @Bean
        @Qualifier("eventStoreDataSource")
        public DataSource eventStoreDataSource() {
            log.info("Configuring PostgreSQL event store datasource: {}", url);
            return DataSourceBuilder.create()
                    .url(url)
                    .username(username)
                    .password(password)
                    .driverClassName("org.postgresql.Driver")
                    .build();
        }

        @Bean
        @Qualifier("eventStoreJdbcTemplate")
        public JdbcTemplate eventStoreJdbcTemplate(@Qualifier("eventStoreDataSource") DataSource dataSource) {
            return new JdbcTemplate(dataSource);
        }

        @Bean
        @Primary
        public EventStore eventStore(@Qualifier("eventStoreJdbcTemplate") JdbcTemplate jdbcTemplate,
                                     ObjectMapper objectMapper) {
            log.info("Using PostgreSQL event store");
            return new PostgresEventStore(jdbcTemplate, objectMapper);
        }

        @Bean
        @Primary
        public FavoritesStore favoritesStore(@Qualifier("eventStoreJdbcTemplate") JdbcTemplate jdbcTemplate,
                                             ObjectMapper objectMapper) {
            log.info("Using PostgreSQL favorites store");
            return new PostgresFavoritesStore(jdbcTemplate, objectMapper);
        }
    }

    // ========== ClickHouse Configuration ==========

    @Configuration
    @ConditionalOnProperty(name = "event.store.type", havingValue = "clickhouse")
    public static class ClickHouseConfig {

        @Value("${event.store.clickhouse.url:jdbc:clickhouse://localhost:8123/default}")
        private String url;

        @Value("${event.store.clickhouse.username:default}")
        private String username;

        @Value("${event.store.clickhouse.password:}")
        private String password;

        @Bean
        @Qualifier("eventStoreDataSource")
        public DataSource eventStoreDataSource() {
            log.info("Configuring ClickHouse event store datasource: {}", url);
            return DataSourceBuilder.create()
                    .url(url)
                    .username(username)
                    .password(password)
                    .driverClassName("com.clickhouse.jdbc.ClickHouseDriver")
                    .build();
        }

        @Bean
        @Qualifier("eventStoreJdbcTemplate")
        public JdbcTemplate eventStoreJdbcTemplate(@Qualifier("eventStoreDataSource") DataSource dataSource) {
            return new JdbcTemplate(dataSource);
        }

        @Bean
        @Primary
        public EventStore eventStore(@Qualifier("eventStoreJdbcTemplate") JdbcTemplate jdbcTemplate,
                                     ObjectMapper objectMapper) {
            log.info("Using ClickHouse event store");
            return new ClickHouseEventStore(jdbcTemplate, objectMapper);
        }

        @Bean
        @Primary
        public FavoritesStore favoritesStore(@Qualifier("eventStoreJdbcTemplate") JdbcTemplate jdbcTemplate,
                                             ObjectMapper objectMapper) {
            log.info("Using ClickHouse favorites store");
            return new ClickHouseFavoritesStore(jdbcTemplate, objectMapper);
        }
    }
}
