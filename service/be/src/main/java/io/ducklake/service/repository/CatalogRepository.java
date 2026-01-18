package io.ducklake.service.repository;

import io.ducklake.service.model.Catalog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface CatalogRepository extends JpaRepository<Catalog, String> {

    List<Catalog> findByEnabledTrue();

    @Query("SELECT c FROM Catalog c WHERE c.enabled = true AND " +
           "(LOWER(c.catalogId) LIKE LOWER(CONCAT('%', :search, '%')) OR " +
           "LOWER(c.displayName) LIKE LOWER(CONCAT('%', :search, '%')))")
    List<Catalog> search(String search);

    Optional<Catalog> findByCatalogIdAndEnabledTrue(String catalogId);
}
