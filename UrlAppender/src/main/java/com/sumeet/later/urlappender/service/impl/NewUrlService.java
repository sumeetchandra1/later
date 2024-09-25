package com.sumeet.later.urlappender.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sumeet.later.urlappender.dto.PaginatedLinksResponse;
import com.sumeet.later.urlappender.model.Link;
import com.sumeet.later.urlappender.repository.impl.NewUrlRepository;
import com.sumeet.later.urlappender.service.RedisCacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@Service
public class NewUrlService {

    private static final Logger logger = LoggerFactory.getLogger(NewUrlService.class);

    private static final String LINKS_SORTED_SET_KEY = "links:sorted";
    private static final String LINKS_HASH_KEY = "links:hash";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final NewUrlRepository urlRepository;
    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisTemplate<String, String> customStringRedisTemplate;
    private final RedisCacheService redisCacheService;

    @Autowired
    public NewUrlService(NewUrlRepository urlRespository,
                         RedisTemplate<String, Object> redisTemplate,
                         RedisTemplate<String, String> customStringRedisTemplate,
                         RedisCacheService redisCacheService) {
        this.urlRepository = urlRespository;
        this.redisTemplate = redisTemplate;
        this.customStringRedisTemplate = customStringRedisTemplate;
        this.redisCacheService = redisCacheService;
    }


    /**
     * Creates a new link or updates an existing link based on the presence of the original URL.
     *
     * @param link the link to create or update
     * @return a CompletableFuture containing the created or updated link
     */
    public CompletableFuture<Link> createOrUpdateLink(Link link) {

        String originalUrl = link.getOriginalUrl();
        logger.info("Processing create or update for originalUrl: {}", originalUrl);

        return urlRepository.getLinkByUrlAsync(originalUrl)
                .thenCompose(existingLink -> {
                    if (existingLink == null) {
                        logger.info("Link does not exist. Creating new link: {}", originalUrl);
                        // Link does not exist; create a new one
                        return createLink(link);
                    } else {
                        logger.info("Link exists. Updating link: {}", originalUrl);
                        // Link exists; update it by appending new parameters
                        return updateLink(existingLink, link.getParameters());
                    }
                })
                .exceptionally(ex -> {
                    logger.error("Error in createOrUpdateLink: {}", ex.getMessage(), ex);
                    throw new CompletionException("Failed to create or update link", ex);
                });
    }

    /**
     * Creates a new link and saves it to the database and Redis cache.
     *
     * @param link the link to create
     * @return a CompletableFuture containing the created link
     */
    private CompletableFuture<Link> createLink(Link link) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Set creation and update timestamps
                long currentTimestamp = System.currentTimeMillis();
                link.setCreationTimestamp(currentTimestamp);
                link.setUpdateTimestamp(currentTimestamp);

                // Generate newUrl by appending parameters
                String newUrl = generateNewUrl(link.getOriginalUrl(), link.getParameters());
                link.setNewUrl(newUrl);

                // Save link to DynamoDB
                urlRepository.saveLink(link);
                logger.info("Link saved to DynamoDB: {}", link.getNewUrl());

                // Cache the new link in Redis
                //redisCacheService.updateRedisCache(link);
                //logger.info("Link cached in Redis: {}", link.getOriginalUrl());

                return link;
            } catch (Exception e) {
                logger.error("Error creating link: {}", e.getMessage(), e);
                throw new CompletionException("Failed to create link", e);
            }
        });
    }

    /**
     * Updates an existing link by appending new parameters to the existing newUrl.
     *
     * @param existingLink the existing link retrieved from DynamoDB
     * @param newParameters the new parameters to append
     * @return a CompletableFuture containing the updated link
     */
    private CompletableFuture<Link> updateLink(Link existingLink, Map<String, String> newParameters) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Merge existing parameters with new parameters
                Map<String, String> updatedParameters = new HashMap<>(existingLink.getParameters());
                updatedParameters.putAll(newParameters);
                existingLink.setParameters(updatedParameters);

                // Generate updated newUrl by appending new parameters
                String updatedNewUrl = generateNewUrl(existingLink.getOriginalUrl(), newParameters, existingLink.getNewUrl());
                existingLink.setNewUrl(updatedNewUrl);

                // Update updateTimestamp
                existingLink.setUpdateTimestamp(System.currentTimeMillis());

                // Save updated link to DynamoDB
                urlRepository.saveLink(existingLink);
                logger.info("Link updated in DynamoDB: {}", existingLink.getOriginalUrl());

                // Update Redis cache
                if(redisCacheService.updateRedisCache(existingLink)) {
                    logger.info("Link cache updated in Redis: {}", existingLink.getOriginalUrl());
                }

                return existingLink;
            } catch (Exception e) {
                logger.error("Error updating link: {}", e.getMessage(), e);
                throw new CompletionException("Failed to update link", e);
            }
        });
    }

    /**
     * Retrieves a paginated list of links ordered by last update time.
     * Utilizes caching with Redis Sorted Sets and Hashes to improve performance.
     *
     * @param limit  the maximum number of links to retrieve
     * @param cursor the pagination cursor
     * @return a CompletableFuture containing the paginated links response
     */
    @Async
    public CompletableFuture<PaginatedLinksResponse> getLinksAsync(int limit, String cursor) {
        logger.info("Retrieving links with limit: {} and cursor: {}", limit, cursor);
        try {
            // Decode cursor to get the last updateTimestamp
            Long lastUpdateTimestamp = null;
            if (cursor != null && !cursor.isEmpty()) {
                lastUpdateTimestamp = decodeCursor(cursor);
                if (lastUpdateTimestamp == null) {
                    logger.warn("Invalid cursor format: {}", cursor);
                    throw new IllegalArgumentException("Invalid cursor format");
                }
            }

            // Fetch originalUrls from Redis Sorted Set
            Set<String> originalUrls = fetchOriginalUrlsFromRedis(lastUpdateTimestamp, limit);

            List<String> linkUrls = new ArrayList<>();
            List<String> missingOriginalUrls = new ArrayList<>();

            // If Redis has no entries, fetch all requested links from DynamoDB
            if (originalUrls.isEmpty()) {
                logger.info("Redis cache is empty. Fetching all requested links from DynamoDB.");
                List<Link> allFetchedLinks = urlRepository.getAllLinksAsync(limit).get(); // Assuming this method returns CompletableFuture<List<Link>>
                for (Link fetchedLink : allFetchedLinks) {
                    linkUrls.add(fetchedLink.getNewUrl());

                    // Cache the fetched link in Redis
                    redisCacheService.addToRedisCache(fetchedLink);
                    logger.info("Fetched and cached link from DynamoDB: {}", fetchedLink.getOriginalUrl());
                }
            } else {
                // Attempt to retrieve link data from Redis Hash
                for (String originalUrl : originalUrls) {
                    String linkJson = (String) redisTemplate.opsForHash().get(LINKS_HASH_KEY, originalUrl);
                    if (linkJson != null) {
                        try {
                            Link link = objectMapper.readValue(linkJson, Link.class);
                            linkUrls.add(link.getNewUrl());
                            logger.info("Retrieved link from Redis Hash: {}", originalUrl);
                        } catch (Exception e) {
                            logger.error("Error parsing link JSON from Redis for URL {}: {}", originalUrl, e.getMessage(), e);
                            // Optionally, handle corrupted cache entries by marking them for refresh
                            missingOriginalUrls.add(originalUrl);
                        }
                    } else {
                        logger.warn("Link data not found in Redis Hash for URL: {}", originalUrl);
                        missingOriginalUrls.add(originalUrl);
                    }
                }

                // If some links are missing in Redis, fetch them from DynamoDB
                if (!missingOriginalUrls.isEmpty()) {
                    logger.info("Fetching {} missing links from DynamoDB.", missingOriginalUrls.size());
                    List<Link> fetchedLinks = urlRepository.getLinksByUrlsAsync(missingOriginalUrls).get(); // Assuming this method returns CompletableFuture<List<Link>>
                    for (Link fetchedLink : fetchedLinks) {
                        linkUrls.add(fetchedLink.getNewUrl());
                        // Cache the fetched link in Redis
                        redisCacheService.addToRedisCache(fetchedLink);
                        logger.info("Fetched and cached link from DynamoDB: {}", fetchedLink.getOriginalUrl());
                    }
                }
            }

            // Determine the new cursor based on the last link's updateTimestamp
            String newCursor = null;
            if (!linkUrls.isEmpty()) {
                String lastUrl = linkUrls.get(linkUrls.size() - 1);
                Link lastLink = getLinkFromCache(lastUrl);
                if (lastLink != null) {
                    newCursor = encodeCursor(lastLink.getUpdateTimestamp());
                    logger.info("Generated new cursor: {}", newCursor);
                }
            }

            PaginatedLinksResponse response = new PaginatedLinksResponse();
            response.setLinks(linkUrls);
            response.setCursor(newCursor);

            return CompletableFuture.completedFuture(response);
        } catch (Exception e) {
            logger.error("Error retrieving paginated links: {}", e.getMessage(), e);
            throw new CompletionException("Failed to retrieve paginated links", e);
        }
    }

    /**
     * Fetches original URLs from Redis Sorted Set based on the lastUpdateTimestamp and limit.
     *
     * @param lastUpdateTimestamp the timestamp to use for pagination
     * @param limit the maximum number of links to retrieve
     * @return a set of original URLs from Redis
     */
    private Set<String> fetchOriginalUrlsFromRedis(Long lastUpdateTimestamp, int limit) {
        if (lastUpdateTimestamp == null) {
            // Fetch the top 'limit' links
            return customStringRedisTemplate.opsForZSet()
                    .reverseRangeByScore(LINKS_SORTED_SET_KEY, Double.MAX_VALUE, 0, 0, limit);
        } else {
            // Fetch links with updateTimestamp < lastUpdateTimestamp
            return customStringRedisTemplate.opsForZSet()
                    .reverseRangeByScore(LINKS_SORTED_SET_KEY, lastUpdateTimestamp - 1, 0, 0, limit);
        }
    }

    /**
     * Updates the Redis cache with the given link.
     *
     * @param link the link to cache
     */
    private void updateRedisCache(Link link) {
        boolean cacheUpdated = redisCacheService.updateRedisCache(link);
        if (cacheUpdated) {
            logger.debug("Link cached successfully in Redis: {}", link.getOriginalUrl());
        } else {
            logger.debug("Link was not cached in Redis as it does not exist: {}", link.getOriginalUrl());
        }
    }

    /**
     * Retrieves a link from Redis Hash based on the newUrl.
     *
     * @param newUrl the newUrl to search for
     * @return the Link object if found, null otherwise
     */
    private Link getLinkFromCache(String newUrl) {

        Link link = null;

        try {
            // Extract originalUrl from newUrl
            String originalUrl = extractOriginalUrl(newUrl);
            if (originalUrl == null) {
                logger.warn("Could not extract originalUrl from newUrl: {}", newUrl);
                return null;
            }

            Object value = redisTemplate.opsForHash().get(LINKS_HASH_KEY, originalUrl);

            // Handle different scenarios based on the type of value retrieved
            if (value == null) {
                logger.warn("Link data not found in Redis Hash for URL: {}", originalUrl);
            } else if (value instanceof Link) {
                // If it's already a Link object, return it directly
                link = (Link) value;
            } else if (value instanceof String) {
                // If it's a JSON string, deserialize it
                link = objectMapper.readValue((String) value, Link.class);
            }

            return link;
        } catch (Exception e) {
            logger.error("Error retrieving link from Redis cache for newUrl {}: {}", newUrl, e.getMessage(), e);
            return null;
        }
    }

    /**
     * Extracts the original URL from the newUrl by removing query parameters.
     *
     * @param newUrl the newUrl containing query parameters
     * @return the original URL without query parameters
     */
    private String extractOriginalUrl(String newUrl) {
        try {
            int index = newUrl.indexOf("?");
            if (index == -1) {
                return newUrl;
            }
            return newUrl.substring(0, index);
        } catch (Exception e) {
            logger.error("Error extracting originalUrl from newUrl {}: {}", newUrl, e.getMessage(), e);
            return null;
        }
    }

    /**
     * Encodes a timestamp into a Base64-encoded cursor.
     *
     * @param timestamp the timestamp to encode
     * @return the encoded cursor
     */
    private String encodeCursor(Long timestamp) {
        try {
            String str = timestamp.toString();
            return Base64.getEncoder().encodeToString(str.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            logger.error("Error encoding cursor from timestamp {}: {}", timestamp, e.getMessage(), e);
            return null;
        }
    }

    /**
     * Decodes a Base64-encoded cursor into a timestamp.
     *
     * @param cursor the Base64-encoded cursor
     * @return the decoded timestamp, or null if decoding fails
     */
    private Long decodeCursor(String cursor) {
        try {
            byte[] decodedBytes = Base64.getDecoder().decode(cursor);
            String str = new String(decodedBytes, StandardCharsets.UTF_8);
            return Long.parseLong(str);
        } catch (Exception e) {
            logger.error("Error decoding cursor {}: {}", cursor, e.getMessage(), e);
            return null;
        }
    }

    /**
     * Generates a new URL by appending parameters to the original URL.
     *
     * @param originalUrl the original URL
     * @param parameters  the parameters to append
     * @return the new URL
     */
    private String generateNewUrl(String originalUrl, Map<String, String> parameters) {
        if (parameters == null || parameters.isEmpty()) {
            return originalUrl;
        }

        StringBuilder newUrl = new StringBuilder(originalUrl);
        if (!originalUrl.contains("?")) {
            newUrl.append("?");
        } else if (!originalUrl.endsWith("&") && !originalUrl.endsWith("?")) {
            newUrl.append("&");
        }

        parameters.forEach((key, value) -> {
            newUrl.append(key).append("=").append(value).append("&");
        });

        // Remove the trailing '&'
        newUrl.setLength(newUrl.length() - 1);

        return newUrl.toString();
    }

    /**
     * Generates a new URL by appending new parameters to the existing newUrl.
     * Preserves existing query parameters.
     *
     * @param originalUrl    the original URL
     * @param newParameters  the new parameters to append
     * @param existingNewUrl the existing newUrl from the database
     * @return the updated newUrl
     */
    private String generateNewUrl(String originalUrl, Map<String, String> newParameters, String existingNewUrl) {
        if (newParameters == null || newParameters.isEmpty()) {
            return existingNewUrl;
        }

        // Parse existing query parameters
        Map<String, String> existingParams = parseQueryParams(existingNewUrl);

        // Merge with new parameters
        existingParams.putAll(newParameters);

        // Reconstruct the newUrl
        StringBuilder newUrl = new StringBuilder(originalUrl);
        if (!originalUrl.contains("?")) {
            newUrl.append("?");
        } else if (!originalUrl.endsWith("&") && !originalUrl.endsWith("?")) {
            newUrl.append("&");
        }

        existingParams.forEach((key, value) -> {
            newUrl.append(key).append("=").append(value).append("&");
        });

        // Remove the trailing '&'
        newUrl.setLength(newUrl.length() - 1);

        return newUrl.toString();
    }

    /**
     * Parses query parameters from a URL into a map.
     *
     * @param url the URL containing query parameters
     * @return a map of query parameter keys and values
     */
    private Map<String, String> parseQueryParams(String url) {
        Map<String, String> params = new HashMap<>();
        try {
            String[] parts = url.split("\\?");
            if (parts.length < 2) {
                return params;
            }
            String query = parts[1];
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                String[] kv = pair.split("=", 2);
                if (kv.length == 2) {
                    params.put(kv[0], kv[1]);
                }
            }
        } catch (Exception e) {
            logger.error("Error parsing query parameters from URL {}: {}", url, e.getMessage(), e);
        }
        return params;
    }
}
