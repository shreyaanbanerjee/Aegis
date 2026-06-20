package com.enterprise.aegis.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Immutable audit record for every LLM request proxied through the Aegis gateway.
 * Written by the BillingKafkaConsumer after processing a TokenUsageEvent.
 * This table provides a complete forensic trail for security review and billing disputes.
 */
@Entity
@Table(name = "audit_logs", indexes = {
        @Index(name = "idx_audit_api_key", columnList = "api_key"),
        @Index(name = "idx_audit_created_at", columnList = "created_at"),
        @Index(name = "idx_audit_department_id", columnList = "department_id")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuditLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * The API key that was used for this request. Stored for quick lookup
     * without joining to the departments table.
     */
    @Column(name = "api_key", nullable = false, length = 256)
    private String apiKey;

    /**
     * FK to the Department that made the request.
     */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "department_id", nullable = false)
    private Department department;

    /**
     * A short summary of the request (first 200 chars of the SANITIZED prompt).
     * IMPORTANT: The original PII-containing prompt is NEVER stored.
     */
    @Column(name = "sanitized_prompt_excerpt", length = 500)
    private String sanitizedPromptExcerpt;

    /**
     * The model identifier returned by the LLM (e.g., "gpt-4o", "gpt-3.5-turbo").
     */
    @Column(name = "model_used", length = 100)
    private String modelUsed;

    /**
     * Prompt tokens consumed as reported by the LLM API.
     */
    @Column(name = "prompt_tokens", nullable = false)
    private int promptTokens;

    /**
     * Completion tokens consumed as reported by the LLM API.
     */
    @Column(name = "completion_tokens", nullable = false)
    private int completionTokens;

    /**
     * Total tokens = promptTokens + completionTokens.
     */
    @Column(name = "total_tokens", nullable = false)
    private int totalTokens;

    /**
     * HTTP status code returned from the upstream LLM (e.g., 200, 429, 500).
     */
    @Column(name = "upstream_status_code")
    private int upstreamStatusCode;

    /**
     * Wall-clock latency in milliseconds for the full round-trip to the LLM.
     */
    @Column(name = "latency_ms")
    private long latencyMs;

    /**
     * Unique correlation ID from the Kafka event, used to correlate logs across services.
     */
    @Column(name = "correlation_id", length = 64)
    private String correlationId;

    /**
     * Timestamp when this audit record was persisted. Set automatically.
     */
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
    }
}
