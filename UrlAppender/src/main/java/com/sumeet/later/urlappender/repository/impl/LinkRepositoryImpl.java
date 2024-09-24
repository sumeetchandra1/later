package com.sumeet.later.urlappender.repository.impl;

import com.sumeet.later.urlappender.model.Link;
import com.sumeet.later.urlappender.service.impl.UrlService;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Repository
public class LinkRepositoryImpl {

    private static final Logger logger = LoggerFactory.getLogger(LinkRepositoryImpl.class);
    private final DynamoDbAsyncTable<Link> linkTable;

    public LinkRepositoryImpl(DynamoDbAsyncClient dynamoDbAsyncClient) {
        DynamoDbEnhancedAsyncClient enhancedClient = DynamoDbEnhancedAsyncClient.builder().dynamoDbClient(dynamoDbAsyncClient).build();
        this.linkTable = enhancedClient.table("Links", TableSchema.fromBean(Link.class));
    }

    /**
     * Fetches a page of links from DynamoDB using pagination tokens.
     *
     * @param exclusiveStartKey The starting point for pagination.
     * @param limit             The maximum number of items to retrieve.
     * @return A CompletableFuture containing a map with "links" and "lastEvaluatedKey".
     */
    public CompletableFuture<Map<String, Object>> getLinkIdsAndLinksAsync(Map<String, AttributeValue> exclusiveStartKey, int limit) {
        ScanEnhancedRequest scanRequest = ScanEnhancedRequest.builder()
                .limit(limit)
                .exclusiveStartKey(exclusiveStartKey)
                .build();

        CompletableFuture<Map<String, Object>> futureResult = new CompletableFuture<>();

        linkTable.scan(scanRequest).subscribe(new Subscriber<Page<Link>>() {
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription s) {
                this.subscription = s;
                s.request(1); // Request one page at a time
            }

            @Override
            public void onNext(Page<Link> page) {
                List<Link> links = page.items();
                Map<String, AttributeValue> lastEvaluatedKey = page.lastEvaluatedKey();

                Map<String, Object> result = new HashMap<>();
                result.put("links", links);
                result.put("lastEvaluatedKey", lastEvaluatedKey);

                futureResult.complete(result);

                // Cancel the subscription since we only need one page
                subscription.cancel();
            }

            @Override
            public void onError(Throwable t) {
                futureResult.completeExceptionally(t);
            }

            @Override
            public void onComplete() {
                // No action needed
            }
        });

        return futureResult;
    }


    /**
     * Retrieves multiple links by their IDs asynchronously.
     *
     * @param ids The list of link IDs to retrieve.
     * @return A CompletableFuture containing a list of Link objects.
     */
    public CompletableFuture<List<Link>> getLinksByIdsAsync(List<String> ids) {
        List<CompletableFuture<Link>> futures = new ArrayList<>();
        for (String id : ids) {
            futures.add(getLinkByIdAsync(id));
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                List<Link> links = new ArrayList<>();
                for (CompletableFuture<Link> future : futures) {
                    try {
                        Link link = future.get();
                        if (link != null) {
                            links.add(link);
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        // Handle exception as needed
                        e.printStackTrace();
                    }
                }
                return links;
            });
    }


    /**
     * Retrieves a single link by its ID asynchronously.
     *
     * @param id The ID of the link to retrieve.
     * @return A CompletableFuture containing the Link object.
     */
    public CompletableFuture<Link> getLinkByIdAsync(String id) {
        Key key = Key.builder().partitionValue(id).build();
        logger.info("Fetching link Id: {} from Dynamo Db", id);
        return linkTable.getItem(GetItemEnhancedRequest.builder().key(key).build());
    }


    /**
     * Saves a link to DynamoDB asynchronously.
     *
     * @param link The Link object to save.
     * @return A CompletableFuture representing the save operation.
     */
    public CompletableFuture<Void> saveLinkAsync(Link link) {
        logger.info("Persisting link Id: {} in Dynamo Db", link.getId());
        return linkTable.putItem(PutItemEnhancedRequest.builder(Link.class).item(link).build());
    }


    /**
     * Updates a link in DynamoDB asynchronously.
     *
     * @param link The Link object to update.
     * @return A CompletableFuture representing the update operation.
     */
    public CompletableFuture<Link> updateLinkAsync(Link link) {
        logger.info("Updating link Id: {} in Dynamo Db", link.getId());
        return linkTable.updateItem(UpdateItemEnhancedRequest.builder(Link.class)
                .item(link)
                .build()).thenApply(updatedItem -> link);
    }
}
