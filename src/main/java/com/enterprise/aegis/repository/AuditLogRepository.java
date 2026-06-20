package com.enterprise.aegis.repository;

import com.enterprise.aegis.entity.AuditLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Spring Data JPA Repository for the AuditLog entity.
 * Audit logs are append-only — this repository provides no update/delete methods.
 * All writes go through the BillingKafkaConsumer.
 */
@Repository
public interface AuditLogRepository extends JpaRepository<AuditLog, Long> {

    /**
     * Retrieves paginated audit logs for a specific department (via API key),
     * ordered by most recent first. Used for admin dashboards.
     *
     * @param apiKey   the department's API key
     * @param pageable pagination and sorting config
     * @return a page of AuditLog records
     */
    Page<AuditLog> findByApiKeyOrderByCreatedAtDesc(String apiKey, Pageable pageable);

    /**
     * Aggregates total tokens consumed by a department within a date range.
     * Useful for generating monthly billing reports.
     *
     * @param apiKey    the department's API key
     * @param startDate inclusive start of the date range
     * @param endDate   inclusive end of the date range
     * @return sum of total tokens consumed; null if no records exist
     */
    @Query("SELECT SUM(a.totalTokens) FROM AuditLog a " +
           "WHERE a.apiKey = :apiKey AND a.createdAt BETWEEN :startDate AND :endDate")
    Long sumTokensByApiKeyAndDateRange(
            @Param("apiKey") String apiKey,
            @Param("startDate") LocalDateTime startDate,
            @Param("endDate") LocalDateTime endDate
    );

    /**
     * Finds all failed LLM calls (non-200 status) for anomaly detection.
     *
     * @param since the cutoff timestamp
     * @return list of failed audit records since the given time
     */
    @Query("SELECT a FROM AuditLog a WHERE a.upstreamStatusCode != 200 AND a.createdAt >= :since")
    List<AuditLog> findFailedCallsSince(@Param("since") LocalDateTime since);
}
