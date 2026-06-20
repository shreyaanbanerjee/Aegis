package com.enterprise.aegis.config;

import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;

/**
 * Spring Cloud Gateway routing configuration.
 *
 * Defines the proxy routes that forward sanitized requests from internal apps
 * to the OpenAI API. All PII masking, auth, and rate limiting are handled by
 * global filters — the routes themselves are intentionally simple.
 *
 * Route Table:
 *  - POST /v1/chat/completions  →  https://api.openai.com/v1/chat/completions
 *  - POST /v1/completions       →  https://api.openai.com/v1/completions
 *  - GET  /v1/models            →  https://api.openai.com/v1/models  (passthrough, no PII)
 *
 * The `Authorization: Bearer {OPENAI_API_KEY}` header is injected by the gateway
 * from the environment variable, so internal apps never need to hold the real key.
 */
@Configuration
public class GatewayConfig {

    @Value("${aegis.openai.api-key:#{environment.OPENAI_API_KEY}}")
    private String openAiApiKey;

    @Value("${aegis.openai.base-url:https://api.openai.com}")
    private String openAiBaseUrl;

    @Bean
    public RouteLocator aegisRouteLocator(RouteLocatorBuilder builder) {
        return builder.routes()
                // ── Chat Completions ────────────────────────────────────────
                .route("openai-chat-completions", r -> r
                        .path("/v1/chat/completions")
                        .and().method("POST")
                        .filters(f -> f
                                // Rewrite the path — strip nothing (paths match exactly)
                                .rewritePath("/v1/(?<segment>.*)", "/v1/${segment}")
                                // Inject the OpenAI API key from the gateway's env
                                .addRequestHeader("Authorization", "Bearer " + openAiApiKey)
                                // Remove the internal X-API-Key header before forwarding
                                .removeRequestHeader("X-API-Key")
                                // Add a custom header to identify traffic from Aegis
                                .addRequestHeader("X-Forwarded-By", "Aegis-Gateway")
                        )
                        .uri(openAiBaseUrl)
                )
                // ── Legacy Completions ─────────────────────────────────────
                .route("openai-completions", r -> r
                        .path("/v1/completions")
                        .and().method("POST")
                        .filters(f -> f
                                .rewritePath("/v1/(?<segment>.*)", "/v1/${segment}")
                                .addRequestHeader("Authorization", "Bearer " + openAiApiKey)
                                .removeRequestHeader("X-API-Key")
                                .addRequestHeader("X-Forwarded-By", "Aegis-Gateway")
                        )
                        .uri(openAiBaseUrl)
                )
                // ── Model Listing (passthrough, no masking needed) ──────────
                .route("openai-models", r -> r
                        .path("/v1/models")
                        .and().method("GET")
                        .filters(f -> f
                                .addRequestHeader("Authorization", "Bearer " + openAiApiKey)
                                .removeRequestHeader("X-API-Key")
                        )
                        .uri(openAiBaseUrl)
                )
                .build();
    }
}
