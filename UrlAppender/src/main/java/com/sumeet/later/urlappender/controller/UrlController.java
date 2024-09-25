package com.sumeet.later.urlappender.controller;


import com.sumeet.later.urlappender.dto.AppendParametersRequest;
import com.sumeet.later.urlappender.dto.AppendParametersResponse;
import com.sumeet.later.urlappender.model.Link;
import com.sumeet.later.urlappender.service.impl.NewUrlService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/v1")
public class UrlController {

    private static final Logger logger = LoggerFactory.getLogger(UrlController.class);
    private final NewUrlService urlService;

    public UrlController(NewUrlService urlService) {
        this.urlService = urlService;
    }

    /**
     * Creates a new link or updates an existing link based on the presence of the original URL.
     *
     * @param request the append parameters request containing URL and parameters
     * @return a CompletableFuture containing the response entity with AppendParametersResponse or error
     */
    @PostMapping("/append-parameters")
    public CompletableFuture<ResponseEntity<?>> createOrUpdateLink(@Valid @RequestBody AppendParametersRequest request) {
        Link link = new Link();
        link.setOriginalUrl(request.getOriginalUrl());
        link.setParameters(request.getParameters());

        return urlService.createOrUpdateLink(link)
                .thenApply(createdOrUpdatedLink -> {
                    AppendParametersResponse response = new AppendParametersResponse();
                    response.setOriginalUrl(createdOrUpdatedLink.getOriginalUrl());
                    response.setParameters(createdOrUpdatedLink.getParameters());
                    response.setNewUrl(createdOrUpdatedLink.getNewUrl());

                    // Determine HTTP status based on whether it was a creation or update
                    HttpStatus status = (createdOrUpdatedLink.getCreationTimestamp().equals(createdOrUpdatedLink.getUpdateTimestamp()))
                            ? HttpStatus.CREATED
                            : HttpStatus.OK;
                    return ResponseEntity.status(status).body(response);
                });
    }

    /**
     * Retrieves a paginated list of links ordered by last update time.
     *
     * @param limit  the maximum number of links to retrieve
     * @param cursor the pagination cursor
     * @return a CompletableFuture containing the response entity with PaginatedLinksResponse or error
     */
    @GetMapping(value = "/links", produces = MediaType.APPLICATION_JSON_VALUE)
    public CompletableFuture<ResponseEntity<?>> getLinks(
            @RequestParam(value = "limit", defaultValue = "10") int limit,
            @RequestParam(value = "cursor", required = false) String cursor) {

        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be greater than 0");
        }

        return urlService.getLinksAsync(limit, cursor)
                .thenApply(ResponseEntity::ok);
    }
}
