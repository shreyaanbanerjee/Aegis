package com.enterprise.aegis.filter;

import com.enterprise.aegis.dto.TokenUsageEvent;
import com.enterprise.aegis.entity.Department;
import com.enterprise.aegis.service.PiiMaskingService;
import com.enterprise.aegis.service.TokenVaultService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * PiiMaskingGatewayFilter — The central privacy enforcement point of Aegis.
 *
 * This GlobalFilter runs AFTER authentication (order = -5) and performs:
 *
 * REQUEST SIDE:
 *  1. Buffers the full request body reactively.
 *  2. Extracts the "messages[].content" fields from the OpenAI-format JSON.
 *  3. Passes each content string through PiiMaskingService.
 *  4. Stores the token-to-PII map in Redis via TokenVaultService.
 *  5. Rewrites the request body with masked content and forwards to OpenAI.
 *
 * RESPONSE SIDE:
 *  6. Intercepts the OpenAI response body via a reactive ServerHttpResponseDecorator.
 *  7. Extracts the response text from "choices[0].message.content".
 *  8. Retrieves the token map from Redis and rehydrates the response text.
 *  9. Publishes a TokenUsageEvent to Kafka for billing.
 * 10. Returns the rehydrated response to the original caller.
 *
 * Thread Safety: Stateless; all state is stored in the ServerWebExchange attributes.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PiiMaskingGatewayFilter implements GlobalFilter, Ordered {

    private static final String ATTR_CORRELATION_ID = "aegis.correlationId";
    private static final String ATTR_MASKED_PROMPT   = "aegis.maskedPrompt";
    private static final String ATTR_REQUEST_START   = "aegis.requestStart";
    private static final String KAFKA_TOPIC           = "token-usage";

    private final PiiMaskingService  maskingService;
    private final TokenVaultService  tokenVaultService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper       objectMapper;

    @Override
    public int getOrder() {
        return -5; // After auth (-10), before routing (0)
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        // Only apply to proxy routes (paths starting with /v1/)
        String path = exchange.getRequest().getPath().toString();
        if (!path.startsWith("/v1/")) {
            return chain.filter(exchange);
        }

        // Generate a unique correlation ID for this request session
        String correlationId = UUID.randomUUID().toString();
        exchange.getAttributes().put(ATTR_CORRELATION_ID, correlationId);
        exchange.getAttributes().put(ATTR_REQUEST_START, System.currentTimeMillis());

        Department dept = exchange.getAttribute(ApiKeyAuthFilter.ATTR_DEPARTMENT);
        String apiKey   = (dept != null) ? dept.getApiKey() : "unknown";

        log.info("Processing request: correlationId={}, department={}, path={}",
                correlationId, dept != null ? dept.getName() : "unknown", path);

        // ── Step 1: Buffer and process the request body ───────────────────────
        return DataBufferUtils.join(exchange.getRequest().getBody())
                .flatMap(dataBuffer -> {
                    byte[] bytes = new byte[dataBuffer.readableByteCount()];
                    dataBuffer.read(bytes);
                    DataBufferUtils.release(dataBuffer);
                    String originalBody = new String(bytes, StandardCharsets.UTF_8);

                    return maskRequestBody(originalBody, correlationId, exchange)
                            .flatMap(maskedBody -> {
                                // Store the masked prompt for audit purposes
                                exchange.getAttributes().put(ATTR_MASKED_PROMPT,
                                        truncate(maskedBody, 200));

                                // Rewrite the request with the masked body
                                ServerHttpRequest mutatedRequest = buildMutatedRequest(
                                        exchange, maskedBody);

                                // Build the response interceptor for rehydration
                                ServerHttpResponse decoratedResponse = buildDecoratedResponse(
                                        exchange, correlationId, apiKey);

                                return chain.filter(
                                        exchange.mutate()
                                                .request(mutatedRequest)
                                                .response(decoratedResponse)
                                                .build()
                                );
                            });
                })
                // Handle cases where the request has no body (e.g., GET passthrough)
                .switchIfEmpty(chain.filter(exchange));
    }

    // ─── Request Masking ─────────────────────────────────────────────────────

    /**
     * Parses the OpenAI-format JSON, masks PII in all message content fields,
     * and returns the rewritten JSON string. Also stores the token map in Redis.
     */
    private Mono<String> maskRequestBody(String body, String correlationId,
                                          ServerWebExchange exchange) {
        try {
            JsonNode root = objectMapper.readTree(body);
            JsonNode messagesNode = root.path("messages");

            if (!messagesNode.isArray()) {
                // Not a standard chat completion request — pass through unmodified
                log.debug("No 'messages' array found in request body, skipping masking");
                return Mono.just(body);
            }

            // Collect all content fields for masking
            StringBuilder allContent = new StringBuilder();
            for (JsonNode msg : messagesNode) {
                String content = msg.path("content").asText("");
                if (!content.isBlank()) {
                    allContent.append(content).append("\n");
                }
            }

            // Run masking (CPU-bound, but fast for typical prompt sizes)
            PiiMaskingService.MaskingResult result = maskingService.mask(allContent.toString());

            // Rewrite each message's content with the masked version
            if (!result.tokenMap().isEmpty()) {
                for (JsonNode msg : messagesNode) {
                    if (msg.has("content") && msg instanceof ObjectNode objMsg) {
                        String originalContent = msg.get("content").asText("");
                        String maskedContent   = maskingService
                                .mask(originalContent).maskedText();
                        objMsg.put("content", maskedContent);
                    }
                }
            }

            // Store the token map in Redis
            return tokenVaultService.storeTokenMap(correlationId, result.tokenMap())
                    .thenReturn(objectMapper.writeValueAsString(root));

        } catch (Exception e) {
            log.error("Failed to parse/mask request body for correlationId={}: {}",
                    correlationId, e.getMessage());
            // Fail-open: forward the original body if masking fails
            // In a strict deployment, change this to return Mono.error(...)
            return Mono.just(body);
        }
    }

    /**
     * Rebuilds the incoming ServerHttpRequest with the masked body and
     * correctly updated Content-Length header.
     */
    private ServerHttpRequest buildMutatedRequest(ServerWebExchange exchange, String newBody) {
        byte[] newBodyBytes = newBody.getBytes(StandardCharsets.UTF_8);
        DataBufferFactory bufferFactory = exchange.getResponse().bufferFactory();
        DataBuffer newDataBuffer = bufferFactory.wrap(newBodyBytes);

        return new ServerHttpRequestDecorator(exchange.getRequest()) {
            @Override
            public Flux<DataBuffer> getBody() {
                return Flux.just(newDataBuffer);
            }

            @Override
            public HttpHeaders getHeaders() {
                HttpHeaders headers = new HttpHeaders();
                headers.putAll(super.getHeaders());
                headers.setContentLength(newBodyBytes.length);
                headers.setContentType(MediaType.APPLICATION_JSON);
                return headers;
            }
        };
    }

    // ─── Response Rehydration ─────────────────────────────────────────────────

    /**
     * Wraps the outgoing response with a decorator that intercepts the body,
     * rehydrates PII tokens, publishes the Kafka billing event, and returns
     * the final response to the client.
     */
    private ServerHttpResponse buildDecoratedResponse(
            ServerWebExchange exchange, String correlationId, String apiKey) {
        return new ServerHttpResponseDecorator(exchange.getResponse()) {
            @Override
            public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
                if (!(body instanceof Flux)) {
                    return super.writeWith(body);
                }

                Flux<DataBuffer> modifiedBody = DataBufferUtils.join((Flux<DataBuffer>) body)
                        .flatMapMany(dataBuffer -> {
                            byte[] bytes = new byte[dataBuffer.readableByteCount()];
                            dataBuffer.read(bytes);
                            DataBufferUtils.release(dataBuffer);
                            String responseBody = new String(bytes, StandardCharsets.UTF_8);

                            return rehydrateAndPublish(
                                    responseBody, correlationId, apiKey, exchange)
                                    .flatMapMany(rehydratedBody -> {
                                        byte[] rehydratedBytes =
                                                rehydratedBody.getBytes(StandardCharsets.UTF_8);
                                        DataBuffer buffer = new DefaultDataBufferFactory()
                                                .wrap(rehydratedBytes);
                                        // Update Content-Length to reflect new body size
                                        getDelegate().getHeaders()
                                                .setContentLength(rehydratedBytes.length);
                                        return Flux.just(buffer);
                                    });
                        });

                return super.writeWith(modifiedBody);
            }
        };
    }

    /**
     * Retrieves the token map from Redis, rehydrates the LLM response body,
     * and asynchronously publishes the TokenUsageEvent to Kafka.
     */
    private Mono<String> rehydrateAndPublish(
            String responseBody, String correlationId, String apiKey,
            ServerWebExchange exchange) {

        return tokenVaultService.retrieveTokenMap(correlationId)
                .flatMap(tokenMap -> {
                    // Parse the OpenAI response to extract content and token usage
                    String rehydratedResponse = responseBody;
                    int promptTokens = 0, completionTokens = 0, totalTokens = 0;
                    String modelUsed = "unknown";

                    try {
                        JsonNode root = objectMapper.readTree(responseBody);
                        modelUsed = root.path("model").asText("unknown");

                        // Rehydrate the response text
                        JsonNode choicesNode = root.path("choices");
                        if (choicesNode.isArray() && !choicesNode.isEmpty()) {
                            JsonNode firstChoice = choicesNode.get(0);
                            JsonNode msgContent  = firstChoice.path("message").path("content");
                            if (!msgContent.isMissingNode()) {
                                String maskedContent    = msgContent.asText();
                                String rehydratedContent = maskingService
                                        .rehydrate(maskedContent, tokenMap);
                                ((ObjectNode) firstChoice.path("message"))
                                        .put("content", rehydratedContent);
                            }
                        }

                        // Extract token usage from the OpenAI response
                        JsonNode usageNode = root.path("usage");
                        promptTokens    = usageNode.path("prompt_tokens").asInt(0);
                        completionTokens = usageNode.path("completion_tokens").asInt(0);
                        totalTokens      = usageNode.path("total_tokens").asInt(0);

                        rehydratedResponse = objectMapper.writeValueAsString(root);

                    } catch (Exception e) {
                        log.error("Failed to rehydrate response for correlationId={}: {}",
                                correlationId, e.getMessage());
                        // Fall through — return the original (still-masked) response
                    }

                    // Calculate request latency
                    Long startMs = exchange.getAttribute(ATTR_REQUEST_START);
                    long latencyMs = startMs != null ? (System.currentTimeMillis() - startMs) : -1;
                    int statusCode = exchange.getResponse().getStatusCode() != null
                            ? exchange.getResponse().getStatusCode().value() : 200;
                    String maskedPromptExcerpt = exchange.getAttribute(ATTR_MASKED_PROMPT);

                    // Publish Kafka billing event asynchronously (fire-and-forget)
                    publishBillingEvent(correlationId, apiKey, modelUsed,
                            promptTokens, completionTokens, totalTokens,
                            maskedPromptExcerpt, statusCode, latencyMs);

                    // Eagerly delete the vault entry after rehydration
                    tokenVaultService.deleteTokenMap(correlationId).subscribe();

                    return Mono.just(rehydratedResponse);
                });
    }

    /**
     * Publishes a TokenUsageEvent to the Kafka "token-usage" topic.
     * This is non-blocking (fire-and-forget from the request thread).
     */
    private void publishBillingEvent(
            String correlationId, String apiKey, String modelUsed,
            int promptTokens, int completionTokens, int totalTokens,
            String sanitizedExcerpt, int statusCode, long latencyMs) {
        try {
            TokenUsageEvent event = TokenUsageEvent.builder()
                    .correlationId(correlationId)
                    .apiKey(apiKey)
                    .modelUsed(modelUsed)
                    .promptTokens(promptTokens)
                    .completionTokens(completionTokens)
                    .totalTokens(totalTokens)
                    .sanitizedPromptExcerpt(sanitizedExcerpt)
                    .upstreamStatusCode(statusCode)
                    .latencyMs(latencyMs)
                    .eventTimestamp(Instant.now().toEpochMilli())
                    .build();

            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(KAFKA_TOPIC, correlationId, payload)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to publish billing event for correlationId={}: {}",
                                    correlationId, ex.getMessage());
                        } else {
                            log.debug("Billing event published: correlationId={}, partition={}, offset={}",
                                    correlationId,
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        }
                    });
        } catch (Exception e) {
            log.error("Failed to serialize billing event for correlationId={}: {}",
                    correlationId, e.getMessage());
        }
    }

    private String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max);
    }
}
