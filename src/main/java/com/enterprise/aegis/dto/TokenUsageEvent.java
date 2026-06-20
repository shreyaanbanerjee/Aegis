package com.enterprise.aegis.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Data Transfer Object representing a token-usage event published to the
 * Kafka topic "token-usage" after each successful LLM proxy call.
 *
 * This object is serialized to JSON by the Kafka producer (in the gateway filter)
 * and deserialized by the BillingKafkaConsumer.
 *
 * Design: Intentionally minimal — no PII, no original prompts.
 * The sanitized excerpt is safe to log for billing audit purposes.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TokenUsageEvent {

    /**
     * Unique correlation ID tying this event to a specific HTTP request.
     * Generated at the gateway filter level and can be used to correlate
     * gateway logs, Kafka events, and audit_log rows.
     */
    @JsonProperty("correlationId")
    private String correlationId;

    /**
     * The API key that authenticated this request.
     * Used by the consumer to look up the owning Department.
     */
    @JsonProperty("apiKey")
    private String apiKey;

    /**
     * The LLM model that was used (e.g., "gpt-4o").
     * Derived from the OpenAI response body's "model" field.
     */
    @JsonProperty("modelUsed")
    private String modelUsed;

    /**
     * Prompt token count from the LLM's usage object.
     */
    @JsonProperty("promptTokens")
    private int promptTokens;

    /**
     * Completion token count from the LLM's usage object.
     */
    @JsonProperty("completionTokens")
    private int completionTokens;

    /**
     * Total token count (promptTokens + completionTokens).
     */
    @JsonProperty("totalTokens")
    private int totalTokens;

    /**
     * First 200 characters of the sanitized (PII-masked) prompt for audit context.
     * NEVER contains original PII — only tokenized placeholders like [PERSON_1].
     */
    @JsonProperty("sanitizedPromptExcerpt")
    private String sanitizedPromptExcerpt;

    /**
     * HTTP status code from the upstream LLM API.
     */
    @JsonProperty("upstreamStatusCode")
    private int upstreamStatusCode;

    /**
     * Latency of the LLM call in milliseconds.
     */
    @JsonProperty("latencyMs")
    private long latencyMs;

    /**
     * Unix epoch timestamp (ms) when the gateway published this event.
     */
    @JsonProperty("eventTimestamp")
    private long eventTimestamp;
}
