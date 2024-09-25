package com.sumeet.later.urlappender.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.sumeet.later.urlappender.model.Link;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Arrays;
import com.sumeet.later.urlappender.config.RedisRetryConfig.RedisRetryable;

@Service
public class RedisCacheService {

    private static final Logger logger = LoggerFactory.getLogger(RedisCacheService.class);
    private static final String LINKS_HASH_KEY = "links:hash";
    private static final String LINKS_SORTED_SET_KEY = "links:sorted";

    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisTemplate<String, String> customStringRedisTemplate;
    private final ObjectMapper objectMapper;

    @Autowired
    public RedisCacheService(RedisTemplate<String, Object> redisTemplate,
                             RedisTemplate<String, String> customStringRedisTemplate) {
        this.redisTemplate = redisTemplate;
        this.customStringRedisTemplate = customStringRedisTemplate;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        this.objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Updates the Redis cache for a given link only if the entry exists.
     * This method will retry up to 3 times with a 1-second x 2 backoff in case of failure.
     *
     * @param link the Link object to update in the cache
     * @return true if the cache was updated, false otherwise
     */
    @RedisRetryable
    public boolean updateRedisCache(Link link) {
        String originalUrl = link.getOriginalUrl();
        try {
            // Define the Lua script --For enhanced performance one
            // can preload this script and get sha1 hash which can
            // be used here to avoid the need to send the script text
            // with each execution
            String script = ""
                    + "if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then "
                    + "  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]); "
                    + "  redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1]); "
                    + "  return 1; "
                    + "else "
                    + "  return 0; "
                    + "end";

            // Create a RedisScript object
            DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
            redisScript.setScriptText(script);
            redisScript.setResultType(Long.class);

            // Serialize the Link object to JSON
            String linkJson = objectMapper.writeValueAsString(link);

            // Execute the script using customStringRedisTemplate
            Long result = customStringRedisTemplate.execute(
                    redisScript,
                    Arrays.asList(LINKS_HASH_KEY, LINKS_SORTED_SET_KEY), // KEYS[1], KEYS[2]
                    originalUrl, // ARGV[1]
                    linkJson,    // ARGV[2]
                    String.valueOf(link.getUpdateTimestamp()) // ARGV[3]
            );

            if (result != null && result == 1) {
                logger.info("Link updated atomically in Redis: {}", originalUrl);
                return true;
            } else {
                logger.info("Link not present in Redis cache; skipping update: {}", originalUrl);
                return false;
            }

        } catch (JsonProcessingException e) {
            logger.error("Error serializing Link object: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to serialize Link object", e);
        } catch (Exception e) {
            logger.error("Error executing Lua script for link {}: {}", originalUrl, e.getMessage(), e);
            throw new RuntimeException("Failed to update Redis cache", e);
        }
    }


    @RedisRetryable
    public boolean addToRedisCache(Link link) {
        String originalUrl = link.getOriginalUrl();
        try {
            // Define the Lua script
            String script = ""
                    + "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]); "
                    + "redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1]); "
                    + "return 1;";

            // Create a RedisScript object
            DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
            redisScript.setScriptText(script);
            redisScript.setResultType(Long.class);

            // Serialize the Link object to JSON
            String linkJson = objectMapper.writeValueAsString(link);

            // Execute the script using customStringRedisTemplate
            Long result = customStringRedisTemplate.execute(
                    redisScript,
                    Arrays.asList(LINKS_HASH_KEY, LINKS_SORTED_SET_KEY), // KEYS[1], KEYS[2]
                    originalUrl, // ARGV[1]
                    linkJson,    // ARGV[2]
                    String.valueOf(link.getUpdateTimestamp()) // ARGV[3]
            );

            if (result != null && result == 1) {
                logger.info("Link added in Redis: {}", originalUrl);
                return true;
            } else {
                logger.error("Unexpected result from Redis script for link: {}", originalUrl);
                return false;
            }

        } catch (JsonProcessingException e) {
            logger.error("Error serializing Link object: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to serialize Link object", e);
        } catch (Exception e) {
            logger.error("Error executing Lua script for link {}: {}", originalUrl, e.getMessage(), e);
            throw new RuntimeException("Failed to update Redis cache", e);
        }
    }
}
