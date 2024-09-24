package com.sumeet.later.urlappender.service;

import com.sumeet.later.urlappender.dto.AppendParametersRequest;
import com.sumeet.later.urlappender.dto.AppendParametersResponse;
import com.sumeet.later.urlappender.dto.PaginatedLinksResponse;
import com.sumeet.later.urlappender.model.Link;


import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface UrlService {

    //CompletableFuture<AppendParametersResponse> appendParameters(AppendParametersRequest request);

    CompletableFuture<Void> saveLink(Link link);

    CompletableFuture<PaginatedLinksResponse> getAllLinks(String exclusiveStartKey, int size);

}
