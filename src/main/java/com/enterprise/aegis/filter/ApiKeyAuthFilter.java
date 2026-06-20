package com.enterprise.aegis.filter;

import com.enterprise.aegis.entity.Department;
import com.enterprise.aegis.repository.DepartmentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Optional;

/**
 * ApiKeyAuthFilter — Global Gateway filter that runs FIRST on every request.
 *
 * Responsibilities:
 *  1. Extracts the X-API-Key header from the incoming request.
 *  2. Validates the key against the PostgreSQL departments table.
 *  3. Rejects inactive or unknown keys with HTTP 401/403.
 *  4. Injects the resolved Department object into the exchange attributes
 *     so downstream filters can access it without additional DB lookups.
 *
 * Ordering: Runs at highest priority (order = -10) before the PII masking filter.
 *
 * IMPORTANT: Database lookup is done on a bounded elastic scheduler to avoid
 * blocking the Netty event loop with JDBC calls.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ApiKeyAuthFilter implements GlobalFilter, Ordered {

    public static final String ATTR_DEPARTMENT = "aegis.department";
    public static final String ATTR_CORRELATION_ID = "aegis.correlationId";
    public static final String HEADER_API_KEY = "X-API-Key";

    private final DepartmentRepository departmentRepository;

    @Override
    public int getOrder() {
        return -10; // Run before all other filters
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        // Only protect /v1/** proxy routes, not static assets or health endpoints
        String path = exchange.getRequest().getPath().toString();
        if (path.startsWith("/actuator") || path.startsWith("/index.html") || path.equals("/")) {
            return chain.filter(exchange);
        }

        String apiKey = exchange.getRequest().getHeaders().getFirst(HEADER_API_KEY);
        if (apiKey == null || apiKey.isBlank()) {
            log.warn("Request rejected: missing X-API-Key header, path={}", path);
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }

        // JPA lookup on a thread from boundedElastic pool to avoid blocking event loop
        return Mono.fromCallable(() -> departmentRepository.findByApiKeyAndActiveTrue(apiKey))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(departmentOpt -> {
                    if (departmentOpt.isEmpty()) {
                        log.warn("Request rejected: invalid or inactive API key=****{}, path={}",
                                maskApiKey(apiKey), path);
                        exchange.getResponse().setStatusCode(HttpStatus.FORBIDDEN);
                        return exchange.getResponse().setComplete();
                    }

                    Department dept = departmentOpt.get();
                    log.debug("Authenticated department='{}' for path={}", dept.getName(), path);

                    // Inject department and a correlation ID into exchange attributes
                    exchange.getAttributes().put(ATTR_DEPARTMENT, dept);

                    return chain.filter(exchange);
                });
    }

    private String maskApiKey(String apiKey) {
        if (apiKey == null || apiKey.length() <= 4) return "****";
        return apiKey.substring(apiKey.length() - 4);
    }
}
