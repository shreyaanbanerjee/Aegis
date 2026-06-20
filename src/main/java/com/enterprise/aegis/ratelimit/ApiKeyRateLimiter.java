package com.enterprise.aegis.ratelimit;

import com.enterprise.aegis.filter.ApiKeyAuthFilter;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * ApiKeyRateLimiter — Resolves the rate limiting key for the RequestRateLimiter filter.
 *
 * The key used for rate limiting is the department's API key, extracted from
 * the X-API-Key request header. This ensures that rate limits are applied
 * per department, not per IP (which could be shared within a corporate network).
 *
 * This bean is used by the Spring Cloud Gateway RequestRateLimiter GatewayFilter,
 * which implements a Token Bucket algorithm in Redis using atomic Lua scripts.
 *
 * If the API key header is missing (should never happen after auth filter),
 * the fallback key is "anonymous" which can be given a very restrictive limit.
 *
 * Redis Keys Used by RequestRateLimiter:
 *  - request_rate_limiter.{key}.tokens  → current token count in the bucket
 *  - request_rate_limiter.{key}.timestamp → last refill timestamp
 */
@Component("apiKeyResolver")
public class ApiKeyRateLimiter implements KeyResolver {

    @Override
    public Mono<String> resolve(ServerWebExchange exchange) {
        // Prefer the authenticated key from the exchange attribute (set by ApiKeyAuthFilter)
        // to avoid re-reading the header after it may have been mutated.
        String headerKey = exchange.getRequest().getHeaders()
                .getFirst(ApiKeyAuthFilter.HEADER_API_KEY);
        return Mono.just(headerKey != null ? headerKey : "anonymous");
    }
}
