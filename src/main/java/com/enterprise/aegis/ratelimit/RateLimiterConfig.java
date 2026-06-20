package com.enterprise.aegis.ratelimit;

import org.springframework.cloud.gateway.filter.ratelimit.RedisRateLimiter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;

/**
 * RateLimiterConfig — Configures the Redis-backed Token Bucket rate limiter.
 *
 * Spring Cloud Gateway's RedisRateLimiter implements the Token Bucket algorithm
 * using atomic Lua scripts executed directly on the Redis server. This guarantees
 * correctness even with multiple gateway replicas sharing the same Redis instance.
 *
 * Token Bucket Parameters:
 *  - replenishRate: tokens added to the bucket per second (steady-state throughput)
 *  - burstCapacity: maximum tokens the bucket can hold (handles traffic spikes)
 *  - requestedTokens: tokens consumed per request (1 for simple rate limiting)
 *
 * These values are configurable via environment variables for different environments:
 *  - Development: high limits (100 req/s)
 *  - Production: tighter limits (10 req/s with burst of 20)
 *
 * The rate limiter is applied to the proxy routes in GatewayConfig via application.yml.
 * When a request exceeds the limit, the gateway returns HTTP 429 Too Many Requests.
 */
@Configuration
public class RateLimiterConfig {

    /** Tokens added to the bucket each second. Controls sustained throughput. */
    @Value("${aegis.rate-limit.replenish-rate:10}")
    private int replenishRate;

    /** Maximum bucket capacity. Controls burst size above the replenish rate. */
    @Value("${aegis.rate-limit.burst-capacity:20}")
    private int burstCapacity;

    /** Tokens consumed per request. Default 1 for standard per-request limiting. */
    @Value("${aegis.rate-limit.requested-tokens:1}")
    private int requestedTokens;

    /**
     * Primary rate limiter bean used by the RequestRateLimiter filter.
     * Configuration is read from application.yml with environment variable overrides.
     */
    @Bean
    public RedisRateLimiter redisRateLimiter() {
        return new RedisRateLimiter(replenishRate, burstCapacity, requestedTokens);
    }
}
