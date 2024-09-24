package com.sumeet.later.urlappender.repository;

import com.sumeet.later.urlappender.model.Link;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import java.util.concurrent.CompletableFuture;

public interface LinkRepository {

    CompletableFuture<Void> save(Link link);

    CompletableFuture<Link> getById(String id);

    CompletableFuture<Page<Link>> findAll(String exclusiveStartKey, int pageSize);
}
