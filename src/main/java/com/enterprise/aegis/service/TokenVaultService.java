package com.enterprise.aegis.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/**
 * TokenVaultService — Reactive Redis integration for PII token storage and retrieval.
 *
 * This service manages the lifecycle of PII token mappings in Redis:
 *  - STORE: Persists a map of [TOKEN → original PII value] with a strict TTL.
 *  - RETRIEVE: Fetches the full map for a given session/correlation ID.
 *  - EXPIRE: TTL is enforced by Redis automatically (default: 60 seconds).
 *
 * Key Design:
 *  Each request is assigned a unique `correlationId`. The Redis key is:
 *    aegis:vault:{correlationId}
 *  The value is a Redis Hash where each field is a token (e.g., "[PERSON_1]")
 *  and the value is the original PII string.
 *
 *  Using a Redis Hash per correlationId (vs. individual string keys per token)
 *  keeps the namespace clean and allows atomic retrieval of the full token map
 *  with a single HGETALL command.
 *
 * Security:
 *  - TTL of 60 seconds ensures PII data is never held in Redis longer than necessary.
 *  - The correlationId is a randomly generated UUID, making vault keys unguessable.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TokenVaultService {

    private final ReactiveStringRedisTemplate redisTemplate;

    /**
     * TTL for token vault entries. Configurable via environment variable.
     * Default: 60 seconds — sufficient for the round-trip to the LLM and back.
     */
    @Value("${aegis.vault.ttl-seconds:60}")
    private long vaultTtlSeconds;

    /** Redis key prefix for all vault entries. */
    private static final String VAULT_KEY_PREFIX = "aegis:vault:";

    // ─── Public API ──────────────────────────────────────────────────────────

    /**
     * Generates a unique correlation ID for a new request session.
     * This ID ties together the vault entry, the Kafka event, and the audit log.
     *
     * @return a random UUID string
     */
    public String generateCorrelationId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Stores the token-to-PII mapping in Redis as a Hash with the configured TTL.
     *
     * @param correlationId unique ID for this request
     * @param tokenMap      map of [TOKEN] → original PII value from the masking service
     * @return a Mono that emits true when the store operation is complete
     */
    public Mono<Boolean> storeTokenMap(String correlationId, Map<String, String> tokenMap) {
        if (tokenMap == null || tokenMap.isEmpty()) {
            log.debug("No tokens to store for correlationId={}", correlationId);
            return Mono.just(true);
        }

        String redisKey = buildKey(correlationId);
        Duration ttl = Duration.ofSeconds(vaultTtlSeconds);

        return redisTemplate.opsForHash()
                .putAll(redisKey, tokenMap)
                .then(redisTemplate.expire(redisKey, ttl))
                .doOnSuccess(result ->
                        log.debug("Stored {} tokens in vault, correlationId={}, ttl={}s",
                                tokenMap.size(), correlationId, vaultTtlSeconds))
                .doOnError(e ->
                        log.error("Failed to store token map in Redis for correlationId={}: {}",
                                correlationId, e.getMessage()));
    }

    /**
     * Retrieves the full token-to-PII map for a given correlation ID.
     * This is called during response rehydration.
     *
     * @param correlationId the unique ID set during the request masking phase
     * @return a Mono emitting the token map, or an empty map if the TTL has expired
     */
    @SuppressWarnings("unchecked")
    public Mono<Map<String, String>> retrieveTokenMap(String correlationId) {
        String redisKey = buildKey(correlationId);
        return redisTemplate.opsForHash()
                .entries(redisKey)
                .collectMap(
                        entry -> (String) entry.getKey(),
                        entry -> (String) entry.getValue()
                )
                .doOnSuccess(map -> {
                    if (map.isEmpty()) {
                        log.warn("Vault miss or TTL expired for correlationId={}. " +
                                 "Response will NOT be rehydrated — PII tokens will appear in output.",
                                correlationId);
                    } else {
                        log.debug("Retrieved {} tokens from vault for correlationId={}",
                                map.size(), correlationId);
                    }
                })
                .doOnError(e ->
                        log.error("Redis error during vault retrieval for correlationId={}: {}",
                                correlationId, e.getMessage()));
    }

    /**
     * Explicitly deletes the vault entry for a correlation ID after rehydration.
     * While Redis TTL handles cleanup automatically, eager deletion reduces
     * the exposure window for PII data in the cache.
     *
     * @param correlationId the unique ID of the vault entry to delete
     * @return a Mono that emits the number of keys deleted (0 or 1)
     */
    public Mono<Long> deleteTokenMap(String correlationId) {
        String redisKey = buildKey(correlationId);
        return redisTemplate.delete(redisKey)
                .doOnSuccess(count ->
                        log.debug("Deleted vault entry for correlationId={}, keys_deleted={}",
                                correlationId, count));
    }

    // ─── Private Helpers ─────────────────────────────────────────────────────

    private String buildKey(String correlationId) {
        return VAULT_KEY_PREFIX + correlationId;
    }
}
