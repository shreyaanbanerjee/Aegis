package com.enterprise.aegis.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;

/**
 * Infrastructure beans for Redis and Jackson.
 *
 * Redis:
 *  - Uses ReactiveStringRedisTemplate for non-blocking operations on the Netty event loop.
 *  - Configured with a String serializer for both keys and values to ensure
 *    data stored by the TokenVaultService is human-readable and inspectable via redis-cli.
 *
 * Jackson:
 *  - Single shared ObjectMapper configured to ignore unknown JSON fields.
 *  - Used by both the gateway filters (for request/response parsing)
 *    and the Kafka consumer (for event deserialization).
 */
@Configuration
public class InfrastructureConfig {

    @Bean
    public ReactiveStringRedisTemplate reactiveStringRedisTemplate(
            ReactiveRedisConnectionFactory factory) {
        StringRedisSerializer serializer = new StringRedisSerializer();
        RedisSerializationContext<String, String> context =
                RedisSerializationContext.<String, String>newSerializationContext(serializer)
                        .key(serializer)
                        .value(serializer)
                        .hashKey(serializer)
                        .hashValue(serializer)
                        .build();
        return new ReactiveStringRedisTemplate(factory, context);
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        // Don't fail on unknown JSON fields — important for forward compatibility
        // with OpenAI API updates that may add new response fields.
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }
}
