package com.sumeet.later.urlappender.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.annotation.Retryable;
import org.springframework.core.annotation.AliasFor;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Configuration
@EnableRetry
public class RedisRetryConfig {

    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @Retryable(
            retryFor = {
                    DataAccessException.class,
                    QueryTimeoutException.class,
                    RedisConnectionFailureException.class,
                    RedisSystemException.class,
                    InvalidDataAccessApiUsageException.class,
                    DataIntegrityViolationException.class
            }
    )
    public @interface RedisRetryable {
        @AliasFor(annotation = Retryable.class, attribute = "maxAttempts")
        int maxAttempts() default 3;

        @AliasFor(annotation = Retryable.class, attribute = "backoff")
        Backoff backoff() default @Backoff(delay = 1000, multiplier = 2);
    }
}