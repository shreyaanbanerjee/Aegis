package com.enterprise.aegis.service;

import com.enterprise.aegis.dto.TokenUsageEvent;
import com.enterprise.aegis.entity.AuditLog;
import com.enterprise.aegis.entity.Department;
import com.enterprise.aegis.repository.AuditLogRepository;
import com.enterprise.aegis.repository.DepartmentRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * Kafka Consumer for the "token-usage" topic.
 *
 * Responsibility:
 *  1. Deserialize the incoming TokenUsageEvent JSON.
 *  2. Look up the Department by its API key.
 *  3. Atomically increment the department's `tokensUsed` counter.
 *  4. Persist an immutable AuditLog record for forensic and billing purposes.
 *
 * Reliability:
 *  - Uses manual acknowledgment (ackMode = MANUAL_IMMEDIATE) so that the Kafka
 *    offset is only committed AFTER the database write succeeds.
 *  - If the DB write fails, the message will be re-delivered on consumer restart,
 *    ensuring at-least-once processing semantics.
 *  - Duplicate events (e.g., from gateway retries) are idempotent because
 *    incrementing a counter and appending an audit row are both safe to repeat.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BillingKafkaConsumer {

    private final DepartmentRepository departmentRepository;
    private final AuditLogRepository auditLogRepository;
    private final ObjectMapper objectMapper;

    /**
     * Listens to the "token-usage" Kafka topic and processes billing events.
     *
     * @param payload         the raw JSON string of the TokenUsageEvent
     * @param kafkaOffset     the Kafka partition offset (for debug logging)
     * @param acknowledgment  manual ACK handle — committed only after successful DB write
     */
    @KafkaListener(
            topics = "${aegis.kafka.topics.token-usage:token-usage}",
            groupId = "${spring.kafka.consumer.group-id:aegis-billing-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void onTokenUsageEvent(
            @Payload String payload,
            @Header(KafkaHeaders.OFFSET) long kafkaOffset,
            Acknowledgment acknowledgment
    ) {
        log.debug("Received token-usage event at offset={}: {}", kafkaOffset, payload);

        TokenUsageEvent event;
        try {
            event = objectMapper.readValue(payload, TokenUsageEvent.class);
        } catch (JsonProcessingException e) {
            // Dead-letter: malformed JSON cannot be retried meaningfully.
            // Log the error and commit the offset to avoid infinite poison-pill loop.
            log.error("Failed to deserialize TokenUsageEvent at offset={}, payload={}. " +
                      "Sending to dead-letter. Error: {}", kafkaOffset, payload, e.getMessage());
            acknowledgment.acknowledge();
            return;
        }

        log.info("Processing billing event: correlationId={}, apiKey=****{}, totalTokens={}",
                event.getCorrelationId(),
                maskApiKey(event.getApiKey()),
                event.getTotalTokens());

        // Step 1: Deduct tokens from the department quota.
        int rowsUpdated = departmentRepository.incrementTokensUsed(
                event.getApiKey(), event.getTotalTokens());

        if (rowsUpdated == 0) {
            log.warn("No active department found for apiKey=****{}. " +
                     "Skipping token deduction but still logging audit record.",
                    maskApiKey(event.getApiKey()));
        }

        // Step 2: Resolve the Department entity for the FK relationship.
        Optional<Department> departmentOpt = departmentRepository
                .findByApiKeyAndActiveTrue(event.getApiKey());

        if (departmentOpt.isEmpty()) {
            log.warn("Cannot create AuditLog: department not found for correlationId={}",
                    event.getCorrelationId());
            acknowledgment.acknowledge();
            return;
        }

        // Step 3: Persist the immutable audit log record.
        AuditLog auditLog = AuditLog.builder()
                .apiKey(event.getApiKey())
                .department(departmentOpt.get())
                .sanitizedPromptExcerpt(truncate(event.getSanitizedPromptExcerpt(), 500))
                .modelUsed(event.getModelUsed())
                .promptTokens(event.getPromptTokens())
                .completionTokens(event.getCompletionTokens())
                .totalTokens(event.getTotalTokens())
                .upstreamStatusCode(event.getUpstreamStatusCode())
                .latencyMs(event.getLatencyMs())
                .correlationId(event.getCorrelationId())
                .build();

        auditLogRepository.save(auditLog);

        log.info("Billing processed: correlationId={}, department={}, tokens={}, auditLogId={}",
                event.getCorrelationId(),
                departmentOpt.get().getName(),
                event.getTotalTokens(),
                auditLog.getId());

        // Step 4: Commit the Kafka offset only after successful DB persistence.
        acknowledgment.acknowledge();
    }

    /**
     * Masks all but the last 4 characters of an API key for safe log output.
     */
    private String maskApiKey(String apiKey) {
        if (apiKey == null || apiKey.length() <= 4) return "****";
        return apiKey.substring(apiKey.length() - 4);
    }

    /**
     * Safely truncates a string to a maximum length for DB storage.
     */
    private String truncate(String str, int maxLength) {
        if (str == null) return null;
        return str.length() <= maxLength ? str : str.substring(0, maxLength);
    }
}
