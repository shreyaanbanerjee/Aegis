package com.enterprise.aegis.repository;

import com.enterprise.aegis.entity.Department;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Spring Data JPA Repository for the Department entity.
 *
 * Provides standard CRUD operations plus custom queries for:
 *  - API key validation (hot path, called on every incoming request)
 *  - Atomic token deduction (called by the Kafka billing consumer)
 */
@Repository
public interface DepartmentRepository extends JpaRepository<Department, Long> {

    /**
     * Finds an active department by its API key.
     * This is called on every incoming request for authentication.
     * The index on `api_key` in the DB schema ensures this is O(1).
     *
     * @param apiKey the raw API key from the X-API-Key request header
     * @return an Optional containing the Department if found and active
     */
    Optional<Department> findByApiKeyAndActiveTrue(String apiKey);

    /**
     * Atomically increments the tokensUsed counter for a department.
     * Using a direct JPQL UPDATE bypasses Hibernate's L1 cache and avoids
     * the read-modify-write cycle, preventing stale data in concurrent scenarios.
     * The @Version field on Department handles optimistic locking at the entity level
     * for broader updates, but for pure counter increments, this is more efficient.
     *
     * @param apiKey     the API key identifying the department
     * @param tokenCount the number of tokens to add to the running total
     * @return the number of rows affected (should always be 1)
     */
    @Modifying
    @Query("UPDATE Department d SET d.tokensUsed = d.tokensUsed + :tokenCount, d.updatedAt = CURRENT_TIMESTAMP " +
           "WHERE d.apiKey = :apiKey AND d.active = true")
    int incrementTokensUsed(@Param("apiKey") String apiKey, @Param("tokenCount") int tokenCount);

    /**
     * Checks whether a department's token quota has been exceeded.
     * Used by the rate limiter as a secondary check beyond Redis bucket limits.
     *
     * @param apiKey the API key to check
     * @return true if the department's tokensUsed >= monthlyTokenQuota
     */
    @Query("SELECT CASE WHEN d.tokensUsed >= d.monthlyTokenQuota THEN true ELSE false END " +
           "FROM Department d WHERE d.apiKey = :apiKey AND d.active = true AND d.monthlyTokenQuota != -1")
    Optional<Boolean> isQuotaExceeded(@Param("apiKey") String apiKey);
}
