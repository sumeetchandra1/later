package com.sumeet.later.urlappender.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sumeet.later.urlappender.model.Link;
import com.sumeet.later.urlappender.dto.PaginatedLinksResponse;
import com.sumeet.later.urlappender.repository.impl.LinkRepositoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import org.springframework.data.redis.core.DefaultTypedTuple;


import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Service
public class UrlService {

    private static final Logger logger = LoggerFactory.getLogger(UrlService.class);
    private final LinkRepositoryImpl linkRepository;
    private final RedisTemplate<String, Object> redisTemplate;

    @Autowired
    public UrlService(LinkRepositoryImpl linkRepository, RedisTemplate<String, Object> redisTemplate) {
        this.linkRepository = linkRepository;
        this.redisTemplate = redisTemplate;
    }

    @Async
    public CompletableFuture<Link> createLinkAsync(Link link) {

        // Set the creation timestamp if it's a new link
        if (link.getCreationTimestamp() == null) {
            link.setCreationTimestamp(System.currentTimeMillis());
        }

        // Check if a link with the same id exists
        return linkRepository.getLinkByIdAsync(link.getId())
            .thenCompose(existingLink -> {
                if (existingLink != null) {
                    // Link with the same id exists
                    logger.info("Link with id {} exists. Merging parameters.", link.getId());

                    // Merge the parameters
                    Map<String, String> existingParams = existingLink.getParameters();
                    Map<String, String> newParams = link.getParameters();

                    // Create a new map to hold the merged parameters
                    Map<String, String> mergedParams = new HashMap<>(existingParams);
                    mergedParams.putAll(newParams);

                    // Update the existing link's parameters
                    existingLink.setParameters(mergedParams);

                    // Reconstruct the newUrl
                    String updatedNewUrl = appendParametersToUrl(existingLink.getOriginalUrl(), mergedParams);
                    logger.info("Updated url is: {}", updatedNewUrl);

                    // Update the existing link's newUrl
                    existingLink.setNewUrl(updatedNewUrl);

                    // Update the link in DynamoDB and cache
                    return linkRepository.updateLinkAsync(existingLink)
                        .thenApply(v -> {
                            // Check if the link is present in the cache
                            Boolean isCached = redisTemplate.opsForHash().hasKey("links", existingLink.getId());

                            if (Boolean.TRUE.equals(isCached)) {
                                // Update the cache
                                redisTemplate.opsForHash().put("links", existingLink.getId(), existingLink);
                                logger.info("Updated link in cache with ID: {}", existingLink.getId());
                            } else {
                                logger.info("Link ID: {} not present in cache; skipping cache update.", existingLink.getId());
                            }

                            logger.info("Updated url in existing link: {}", existingLink.getNewUrl());
                            // Return the updated link
                            return existingLink;
                        });
                } else {
                    // Link with the same id does not exist
                    logger.info("Link with id {} does not exist. Creating new link.", link.getId());

                    // Generate the newUrl by appending parameters to originalUrl
                    String newUrl = appendParametersToUrl(link.getOriginalUrl(), link.getParameters());
                    link.setNewUrl(newUrl);

                    // Save to DynamoDB
                    return linkRepository.saveLinkAsync(link).thenApply(v-> {return link;});
                }
            });
    }

    @Async
    public CompletableFuture<PaginatedLinksResponse> getLinksAsync(int limit, String cursor) {
        ZSetOperations<String, Object> zSetOps = redisTemplate.opsForZSet();
        HashOperations<String, Object, Object> hashOps = redisTemplate.opsForHash();

        // Initialize variables
        final long startIndex;
        final Map<String, AttributeValue> exclusiveStartKey;

        try {
            // Determine the startIndex or exclusiveStartKey based on the cursor
            if (cursor != null && !cursor.isEmpty()) {
                if (isNumericCursor(cursor)) {
                    startIndex = Long.parseLong(cursor);
                    exclusiveStartKey = null;
                } else {
                    startIndex = 0;
                    exclusiveStartKey = parseCursor(cursor);
                }
            } else {
                startIndex = 0;
                exclusiveStartKey = null;
            }
        } catch (NumberFormatException e) {
            logger.error("Invalid cursor format: {}", cursor, e);
            throw new IllegalArgumentException("Invalid cursor format", e);
        } catch (Exception e) {
            logger.error("Error parsing cursor: {}", cursor, e);
            throw new RuntimeException("Error parsing cursor", e);
        }

        long endIndex = startIndex + limit - 1;

        // Attempt to fetch link IDs from Redis sorted set
        Set<Object> idSet;
        try {
            idSet = zSetOps.range("linkIds", startIndex, endIndex);
        } catch (Exception e) {
            logger.error("Error accessing Redis sorted set 'linkIds'", e);
            throw new RuntimeException("Error accessing Redis", e);
        }

        List<Link> links = new ArrayList<>();
        List<String> missingIds = new ArrayList<>();
        final List<String> ids; // Declare 'ids' as final

        if (idSet != null && !idSet.isEmpty()) {
            ids = idSet.stream().map(Object::toString).collect(Collectors.toList());

            // Fetch link data from Redis hash
            List<Object> cachedLinks;
            try {
                cachedLinks = hashOps.multiGet("links", new ArrayList<>(ids));
            } catch (Exception e) {
                logger.error("Error accessing Redis hash 'links'", e);
                throw new RuntimeException("Error accessing Redis", e);
            }

            for (int i = 0; i < ids.size(); i++) {
                Object obj = cachedLinks.get(i);
                if (obj != null) {
                    Link link = (Link) obj;
                    links.add(link);
                } else {
                    missingIds.add(ids.get(i));
                }
            }
        } else {
            ids = new ArrayList<>();
        }

        long linksFetched = links.size();

        if (linksFetched < limit) {
            int remaining = limit - (int) linksFetched;

            // Only parse the cursor for DynamoDB if we didn't get any links from Redis
            Map<String, AttributeValue> dynamoExclusiveStartKey = exclusiveStartKey;
            if (dynamoExclusiveStartKey == null && cursor != null && !cursor.isEmpty() && linksFetched == 0) {
                try {
                    dynamoExclusiveStartKey = parseCursor(cursor);
                } catch (Exception e) {
                    logger.error("Error parsing cursor for DynamoDB: {}", cursor, e);
                    throw new RuntimeException("Error parsing cursor for DynamoDB", e);
                }
            }

            // Capture final variables for use inside the lambda
            final Map<String, AttributeValue> finalExclusiveStartKey = dynamoExclusiveStartKey;
            final List<Link> finalLinks = links;
            final List<String> finalIds = ids;

            return linkRepository.getLinkIdsAndLinksAsync(finalExclusiveStartKey, remaining)
                    .handle((result, ex) -> {
                        if (ex != null) {
                            logger.error("Error fetching links from DynamoDB", ex);
                            throw new CompletionException(ex);
                        }

                        @SuppressWarnings("unchecked")
                        List<Link> dynamoLinks = (List<Link>) result.get("links");
                        @SuppressWarnings("unchecked")
                        Map<String, AttributeValue> lastEvaluatedKey = (Map<String, AttributeValue>) result.get("lastEvaluatedKey");

                        // Update cache with new links
                        if (dynamoLinks != null && !dynamoLinks.isEmpty()) {
                            Map<String, Link> linkMap = dynamoLinks.stream()
                                    .collect(Collectors.toMap(Link::getId, link -> link));
                            try {
                                hashOps.putAll("links", linkMap);
                                logger.info("Cached {} links from DynamoDB.", dynamoLinks.size());

                                // Update the sorted set with new IDs
                                Map<String, Double> idScoreMap = new HashMap<>();
                                for (Link link : dynamoLinks) {
                                    String id = link.getId();
                                    double score = link.getCreationTimestamp() != null ? link.getCreationTimestamp() : System.currentTimeMillis();
                                    idScoreMap.put(id, score);
                                }
                                // Convert idScoreMap to Set<TypedTuple<Object>>
                                Set<ZSetOperations.TypedTuple<Object>> tuples = convertToTypedTuples(idScoreMap);
                                zSetOps.add("linkIds", tuples);
                                logger.info("Added {} link IDs to Redis sorted set.", idScoreMap.size());
                            } catch (Exception cacheEx) {
                                logger.error("Error updating Redis cache with data from DynamoDB", cacheEx);
                                throw new CompletionException(cacheEx);
                            }

                            // Add new links to the result list
                            finalLinks.addAll(dynamoLinks);
                        }

                        // Prepare next cursor
                        String nextCursor = null;
                        if (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
                            // More data in DynamoDB
                            nextCursor = encodeCursor(lastEvaluatedKey);
                        } else {
                            // Use the next index in Redis
                            Long totalSize = zSetOps.size("linkIds");
                            long nextIndex = startIndex + finalLinks.size();
                            nextCursor = nextIndex < totalSize ? String.valueOf(nextIndex) : null;
                        }

                        // Combine IDs from Redis and DynamoDB for sorting
                        List<String> combinedIds = new ArrayList<>(finalIds);
                        if (dynamoLinks != null && !dynamoLinks.isEmpty()) {
                            combinedIds.addAll(dynamoLinks.stream().map(Link::getId).collect(Collectors.toList()));
                        }

                        // Sort links based on the original order
                        finalLinks.sort(Comparator.comparingInt(link -> combinedIds.indexOf(link.getId())));

                        PaginatedLinksResponse response = new PaginatedLinksResponse();
                        response.setLinks(finalLinks);
                        response.setCursor(nextCursor);
                        return response;
                    });
        } else {
            // We have enough links from Redis
            // Prepare next cursor
            Long totalSize = zSetOps.size("linkIds");
            long nextIndex = startIndex + limit;
            String nextCursor = nextIndex < totalSize ? String.valueOf(nextIndex) : null;

            // Sort links based on the original order
            links.sort(Comparator.comparingInt(link -> ids.indexOf(link.getId())));

            PaginatedLinksResponse response = new PaginatedLinksResponse();
            response.setLinks(links);
            response.setCursor(nextCursor);
            return CompletableFuture.completedFuture(response);
        }
    }



    private Set<ZSetOperations.TypedTuple<Object>> convertToTypedTuples(Map<?, Double> idScoreMap) {
        return idScoreMap.entrySet().stream()
                .map(entry -> new DefaultTypedTuple<Object>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toSet());
    }

    private boolean isNumericCursor(String cursor) {
        try {
            Long.parseLong(cursor);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private String encodeCursor(Map<String, AttributeValue> lastEvaluatedKey) {
        try {
            Map<String, Map<String, Object>> serializableMap = new HashMap<>();
            for (Map.Entry<String, AttributeValue> entry : lastEvaluatedKey.entrySet()) {
                serializableMap.put(entry.getKey(), attributeValueToMap(entry.getValue()));
            }
            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(serializableMap);
            return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to encode cursor", e);
        }
    }


    private Map<String, AttributeValue> parseCursor(String cursor) {
        try {
            byte[] bytes = Base64.getDecoder().decode(cursor);
            String json = new String(bytes, StandardCharsets.UTF_8);
            ObjectMapper objectMapper = new ObjectMapper();
            TypeReference<Map<String, Map<String, Object>>> typeRef = new TypeReference<Map<String, Map<String, Object>>>() {};
            Map<String, Map<String, Object>> serializableMap = objectMapper.readValue(json, typeRef);

            Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
            for (Map.Entry<String, Map<String, Object>> entry : serializableMap.entrySet()) {
                lastEvaluatedKey.put(entry.getKey(), mapToAttributeValue(entry.getValue()));
            }
            return lastEvaluatedKey;
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse cursor", e);
        }
    }


    private Map<String, Object> attributeValueToMap(AttributeValue attributeValue) {
        Map<String, Object> map = new HashMap<>();
        if (attributeValue.s() != null) {
            map.put("S", attributeValue.s());
        } else if (attributeValue.n() != null) {
            map.put("N", attributeValue.n());
        } else if (attributeValue.b() != null) {
            map.put("B", attributeValue.b().asUtf8String());
        } else if (attributeValue.bool() != null) {
            map.put("BOOL", attributeValue.bool());
        } else if (attributeValue.hasM()) {
            Map<String, Map<String, Object>> nestedMap = new HashMap<>();
            for (Map.Entry<String, AttributeValue> entry : attributeValue.m().entrySet()) {
                nestedMap.put(entry.getKey(), attributeValueToMap(entry.getValue()));
            }
            map.put("M", nestedMap);
        } else if (attributeValue.hasL()) {
            List<Map<String, Object>> list = new ArrayList<>();
            for (AttributeValue av : attributeValue.l()) {
                list.add(attributeValueToMap(av));
            }
            map.put("L", list);
        }
        // Handle other types as needed
        return map;
    }


    private AttributeValue mapToAttributeValue(Map<String, Object> map) {
        if (map.containsKey("S")) {
            return AttributeValue.builder().s((String) map.get("S")).build();
        } else if (map.containsKey("N")) {
            return AttributeValue.builder().n((String) map.get("N")).build();
        } else if (map.containsKey("B")) {
            return AttributeValue.builder().b(SdkBytes.fromUtf8String((String) map.get("B"))).build();
        } else if (map.containsKey("BOOL")) {
            return AttributeValue.builder().bool((Boolean) map.get("BOOL")).build();
        } else if (map.containsKey("M")) {
            Map<String, AttributeValue> nestedMap = new HashMap<>();
            Map<String, Map<String, Object>> m = (Map<String, Map<String, Object>>) map.get("M");
            for (Map.Entry<String, Map<String, Object>> entry : m.entrySet()) {
                nestedMap.put(entry.getKey(), mapToAttributeValue(entry.getValue()));
            }
            return AttributeValue.builder().m(nestedMap).build();
        } else if (map.containsKey("L")) {
            List<Map<String, Object>> list = (List<Map<String, Object>>) map.get("L");
            List<AttributeValue> avList = new ArrayList<>();
            for (Map<String, Object> item : list) {
                avList.add(mapToAttributeValue(item));
            }
            return AttributeValue.builder().l(avList).build();
        }
        // Handle other types as needed
        throw new IllegalArgumentException("Unsupported AttributeValue type");
    }


    /**
     * Appends parameters to a URL while preserving existing query parameters.
     *
     * @param url        The original URL.
     * @param parameters The parameters to append.
     * @return The updated URL with appended parameters.
     */
    private String appendParametersToUrl(String url, Map<String, String> parameters) {
        try {
            // Parse the existing URL
            java.net.URI uri = new java.net.URI(url);
            String query = uri.getQuery();

            // Existing query parameters
            Map<String, String> queryPairs = new LinkedHashMap<>();
            if (query != null && !query.isEmpty()) {
                queryPairs.putAll(splitQuery(query));
            }

            // Add new parameters
            queryPairs.putAll(parameters);

            // Build the new query string
            StringBuilder newQuery = new StringBuilder();
            for (Map.Entry<String, String> entry : queryPairs.entrySet()) {
                if (newQuery.length() > 0) {
                    newQuery.append("&");
                }
                newQuery.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
                newQuery.append("=");
                newQuery.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
            }

            // Build the new URI
            java.net.URI newUri = new java.net.URI(
                    uri.getScheme(),
                    uri.getAuthority(),
                    uri.getPath(),
                    newQuery.toString(),
                    uri.getFragment());

            return newUri.toString();
        } catch (Exception e) {
            // Handle exception appropriately
            throw new RuntimeException("Invalid URL syntax: " + url, e);
        }
    }

    /**
     * Splits a query string into a map of key-value pairs.
     *
     * @param query The query string.
     * @return A map of query parameters.
     */
    private Map<String, String> splitQuery(String query) {
        Map<String, String> queryPairs = new LinkedHashMap<>();
        String[] pairs = query.split("&");
        try {
            for (String pair : pairs) {
                int idx = pair.indexOf("=");
                String key = idx > 0 ? java.net.URLDecoder.decode(pair.substring(0, idx), "UTF-8") : pair;
                String value = idx > 0 && pair.length() > idx + 1 ? java.net.URLDecoder.decode(pair.substring(idx + 1), "UTF-8") : "";
                queryPairs.put(key, value);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error parsing query parameters", e);
        }
        return queryPairs;
    }
}
