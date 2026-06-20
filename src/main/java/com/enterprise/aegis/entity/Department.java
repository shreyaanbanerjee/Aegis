package com.enterprise.aegis.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Represents a corporate department that has been onboarded to the Aegis gateway.
 * Each department is issued a unique API key and is assigned a monthly token quota.
 * Token usage is deducted from `tokensUsed` by the BillingKafkaConsumer.
 */
@Entity
@Table(name = "departments")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Department {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Human-readable department name (e.g., "Engineering", "HR", "Finance").
     */
    @Column(nullable = false, unique = true, length = 100)
    private String name;

    /**
     * The API key issued to this department.
     * This key is passed in the X-API-Key header by internal apps.
     * Stored here for key validation and billing attribution.
     */
    @Column(name = "api_key", nullable = false, unique = true, length = 256)
    private String apiKey;

    /**
     * Maximum number of LLM tokens this department can consume per billing cycle.
     * A value of -1 indicates an unlimited quota (for admin departments).
     */
    @Column(name = "monthly_token_quota", nullable = false)
    @Builder.Default
    private long monthlyTokenQuota = 1_000_000L;

    /**
     * Cumulative tokens consumed in the current billing cycle.
     * This is atomically incremented by the Kafka billing consumer.
     * Uses optimistic locking via @Version to prevent race conditions.
     */
    @Column(name = "tokens_used", nullable = false)
    @Builder.Default
    private long tokensUsed = 0L;

    /**
     * Soft-delete / active flag. Inactive departments are rejected at the auth filter.
     */
    @Column(nullable = false)
    @Builder.Default
    private boolean active = true;

    /**
     * Timestamp when this department record was created.
     */
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    /**
     * Timestamp of the last update (billing deduction, key rotation, etc.).
     */
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    /**
     * Optimistic locking version field to prevent concurrent billing update conflicts.
     */
    @Version
    private Long version;

    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
        this.updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }
}
