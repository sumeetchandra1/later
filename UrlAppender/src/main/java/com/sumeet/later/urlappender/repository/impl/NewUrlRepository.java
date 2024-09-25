package com.sumeet.later.urlappender.repository.impl;

import com.sumeet.later.urlappender.model.Link;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

@Repository
public class NewUrlRepository {

    private static final Logger logger = LoggerFactory.getLogger(NewUrlRepository.class);

    private static final String TABLE_NAME = "Links";

    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final ObjectMapper objectMapper;

    @Autowired
    public NewUrlRepository(DynamoDbAsyncClient dynamoDbAsyncClient) {
        this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    /**
     * Fetches a single link by its originalUrl asynchronously.
     *
     * @param originalUrl the original URL to fetch
     * @return a CompletableFuture containing the Link object, or null if not found
     */
    public CompletableFuture<Link> getLinkByUrlAsync(String originalUrl) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("originalUrl", AttributeValue.builder().s(originalUrl).build());

        GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName(TABLE_NAME)
                .key(key)
                .consistentRead(true)
                .build();

        return dynamoDbAsyncClient.getItem(getItemRequest)
                .thenApply(GetItemResponse::item)
                .thenApply(item -> {
                    if (item == null || item.isEmpty()) {
                        return null;
                    }
                    try {
                        return objectMapper.readValue(AttributeValueToMap(item), Link.class);
                    } catch (Exception e) {
                        // Handle deserialization error
                        // Could log and return null or throw a runtime exception
                        e.printStackTrace();
                        return null;
                    }
                });
    }

    /**
     * Fetches multiple links by their originalUrls asynchronously.
     *
     * @param originalUrls the list of original URLs to fetch
     * @return a CompletableFuture containing the list of Link objects
     */
    public CompletableFuture<List<Link>> getLinksByUrlsAsync(List<String> originalUrls) {
        if (originalUrls == null || originalUrls.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        // Prepare keys for BatchGetItem
        List<Map<String, AttributeValue>> keys = originalUrls.stream()
                .map(url -> {
                    Map<String, AttributeValue> key = new HashMap<>();
                    key.put("originalUrl", AttributeValue.builder().s(url).build());
                    return key;
                })
                .collect(Collectors.toList());

        // DynamoDB BatchGetItem allows up to 100 items per request
        final int batchSize = 100;
        List<CompletableFuture<List<Link>>> futures = new ArrayList<>();

        for (int i = 0; i < keys.size(); i += batchSize) {
            int end = Math.min(i + batchSize, keys.size());
            List<Map<String, AttributeValue>> batchKeys = keys.subList(i, end);

            BatchGetItemRequest batchGetItemRequest = BatchGetItemRequest.builder()
                    .requestItems(Collections.singletonMap(TABLE_NAME, KeysAndAttributes.builder()
                            .keys(batchKeys)
                            .build()))
                    .build();

            CompletableFuture<BatchGetItemResponse> futureResponse = dynamoDbAsyncClient.batchGetItem(batchGetItemRequest);

            CompletableFuture<List<Link>> futureLinks = futureResponse.thenCompose(response -> {
                List<Link> links = new ArrayList<>();

                // Process the responses
                List<Map<String, AttributeValue>> items = response.responses().getOrDefault(TABLE_NAME, Collections.emptyList());
                for (Map<String, AttributeValue> item : items) {
                    try {
                        Link link = objectMapper.readValue(AttributeValueToMap(item), Link.class);
                        links.add(link);
                    } catch (Exception e) {
                        // Handle deserialization error
                        e.printStackTrace();
                    }
                }

                // Handle UnprocessedKeys if necessary
                if (response.unprocessedKeys().containsKey(TABLE_NAME) && !response.unprocessedKeys().get(TABLE_NAME).keys().isEmpty()) {
                    // TODO: Implement retry logic for unprocessed keys if needed
                }

                return CompletableFuture.completedFuture(links);
            });

            futures.add(futureLinks);
        }

        // Combine all futures
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    List<Link> allLinks = new ArrayList<>();
                    for (CompletableFuture<List<Link>> future : futures) {
                        try {
                            allLinks.addAll(future.get());
                        } catch (Exception e) {
                            // Handle exceptions from individual batch requests
                            e.printStackTrace();
                        }
                    }
                    return allLinks;
                });
    }

    /**
     * Fetches a paginated list of links from DynamoDB asynchronously.
     *
     * @param limit the maximum number of links to retrieve
     * @return a CompletableFuture containing the list of Link objects
     */
    public CompletableFuture<List<Link>> getAllLinksAsync(int limit) {
        List<Link> allLinks = new ArrayList<>();
        return fetchLinksRecursively(limit, allLinks, null);
    }

    /*
    Notes:
    Performance: Scanning and sorting in memory is less efficient than using a GSI,
                 especially for large tables. One should create a GSI on updateTimestamp for better performance if possible.
    Consistency: Here we are using consistent reads (consistentRead(true)),
                 which ensures we get the most up-to-date data but at the cost of higher read capacity consumption.
    Recursive Approach: The method uses recursion to handle pagination in DynamoDB.
                        This is because a single scan operation might not return all the requested items, especially if the table is large.
    Asynchronous Operation: It uses CompletableFuture to perform operations asynchronously,
                            which can improve performance by not blocking the thread while waiting for the DynamoDB response.
     */
    private CompletableFuture<List<Link>> fetchLinksRecursively(int limit, List<Link> allLinks, Map<String, AttributeValue> exclusiveStartKey) {
        ScanRequest.Builder scanBuilder = ScanRequest.builder()
                .tableName(TABLE_NAME)
                .limit(limit - allLinks.size())
                .scanFilter(
                        Map.of("updateTimestamp",
                                Condition.builder()
                                        .comparisonOperator(ComparisonOperator.NOT_NULL)
                                        .build())
                )
                .consistentRead(true);

        if (exclusiveStartKey != null) {
            scanBuilder.exclusiveStartKey(exclusiveStartKey);
        }

        ScanRequest scanRequest = scanBuilder.build();
        return dynamoDbAsyncClient.scan(scanRequest)
                .thenCompose(scanResponse -> {
                    List<Map<String, AttributeValue>> items = scanResponse.items();
                    items.sort((item1, item2) -> {
                        Long timestamp1 = item1.containsKey("updateTimestamp") ? Long.parseLong(item1.get("updateTimestamp").n()) : 0L;
                        Long timestamp2 = item2.containsKey("updateTimestamp") ? Long.parseLong(item2.get("updateTimestamp").n()) : 0L;
                        return timestamp2.compareTo(timestamp1); // Descending order
                    });

                    for (Map<String, AttributeValue> item : items) {
                        try {
                            Link link = convertToLink(item);
                            allLinks.add(link);
                            if (allLinks.size() >= limit) {
                                break;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    if (allLinks.size() >= limit || scanResponse.lastEvaluatedKey() == null || scanResponse.lastEvaluatedKey().isEmpty()) {
                        return CompletableFuture.completedFuture(allLinks);
                    } else {
                        return fetchLinksRecursively(limit, allLinks, scanResponse.lastEvaluatedKey());
                    }
                }).exceptionally(e -> {
                    e.printStackTrace();
                    return allLinks; // Or handle the error as needed
                });
    }

    /*
    The convertToLink method directly converts DynamoDB AttributeValue to Link objects using the builder pattern.
     */
    private Link convertToLink(Map<String, AttributeValue> item) {
        return Link.builder()
                .originalUrl(item.get("originalUrl").s())
                .newUrl(item.get("newUrl").s())
                .parameters(convertToStringMap(item.get("parameters").m()))
                .creationTimestamp(Long.parseLong(item.get("creationTimestamp").n()))
                .updateTimestamp(Long.parseLong(item.get("updateTimestamp").n()))
                .build();
    }

    /*
    The convertToStringMap method handles the conversion of the parameters map.
     */
    private Map<String, String> convertToStringMap(Map<String, AttributeValue> map) {
        return map.entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().s()
                ));
    }

    /**
     * Saves or updates a link in DynamoDB asynchronously.
     *
     * @param link the Link object to save
     */
    public CompletableFuture<Void> saveLink(Link link) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("originalUrl", AttributeValue.builder().s(link.getOriginalUrl()).build());
        item.put("newUrl", AttributeValue.builder().s(link.getNewUrl()).build());
        item.put("parameters", AttributeValue.builder().m(link.getParameters().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> AttributeValue.builder().s(e.getValue()).build()
                ))).build());
        item.put("creationTimestamp", AttributeValue.builder().n(String.valueOf(link.getCreationTimestamp())).build());
        item.put("updateTimestamp", AttributeValue.builder().n(String.valueOf(link.getUpdateTimestamp())).build());

        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(TABLE_NAME)
                .item(item)
                .build();

        return dynamoDbAsyncClient.putItem(putItemRequest)
                .thenAccept(response -> {
                    logger.info("Saved link to DynamoDB: {}", link.getNewUrl());
                })
                .exceptionally(e -> {
                    logger.error("Error saving link to DynamoDB: {}", e.getMessage(), e);
                    throw new CompletionException(e);
                });
    }

    /**
     * Helper method to convert DynamoDB AttributeValue map to a JSON string.
     *
     * @param item the DynamoDB item map
     * @return JSON string representation of the item
     * @throws Exception if serialization fails
     */
    private String AttributeValueToMap(Map<String, AttributeValue> item) throws Exception {
        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            map.put(entry.getKey(), AttributeValueToJava(entry.getValue()));
        }
        return objectMapper.writeValueAsString(map);
    }

    /**
     * Converts a DynamoDB AttributeValue to a Java Object.
     *
     * @param attributeValue the DynamoDB AttributeValue
     * @return the corresponding Java Object
     */
    private Object AttributeValueToJava(AttributeValue attributeValue) {
        if (attributeValue.s() != null) {
            return attributeValue.s();
        } else if (attributeValue.n() != null) {
            try {
                return Long.parseLong(attributeValue.n());
            } catch (NumberFormatException e) {
                return attributeValue.n();
            }
        } else if (attributeValue.m() != null) {
            Map<String, AttributeValue> m = attributeValue.m();
            Map<String, Object> map = new HashMap<>();
            for (Map.Entry<String, AttributeValue> entry : m.entrySet()) {
                map.put(entry.getKey(), AttributeValueToJava(entry.getValue()));
            }
            return map;
        }
        return null;
    }
}
