package com.sumeet.later.urlappender.controller;

import com.sumeet.later.urlappender.dto.AppendParametersRequest;
import com.sumeet.later.urlappender.dto.AppendParametersResponse;
import com.sumeet.later.urlappender.dto.ErrorResponse;
import com.sumeet.later.urlappender.dto.PaginatedLinksResponse;
import com.sumeet.later.urlappender.model.Link;
import com.sumeet.later.urlappender.service.impl.UrlService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api")
public class UrlController {

    private static final Logger logger = LoggerFactory.getLogger(UrlController.class);
    private final UrlService urlService;

    public UrlController(UrlService urlService) {
        this.urlService = urlService;
    }

    @PostMapping("/append-parameters")
    public CompletableFuture<ResponseEntity<?>>  appendParameters(@Valid @RequestBody AppendParametersRequest request) {

        // Map AppendParametersRequest to a Link object
        Link link = new Link();
        link.setId(request.getId());
        link.setOriginalUrl(request.getUrl());
        link.setParameters(request.getParameters());

        // Call the service to save the Link and process URL and parameters
        return urlService.createLinkAsync(link)
            .handle((result, throwable) -> {
                if (throwable == null) {
                    // Success case: Create a successful AppendParametersResponse
                    AppendParametersResponse response = new AppendParametersResponse();
                    response.setOriginalUrl(link.getOriginalUrl());
                    response.setParameters(link.getParameters());
                    response.setNewUrl(link.getNewUrl());

                    // Return a ResponseEntity with the AppendParametersResponse
                    return ResponseEntity.ok(response);  // HTTP 200 OK

                } else {
                    // Error case: Create and return an ErrorResponse
                    ErrorResponse errorResponse = new ErrorResponse();
                    errorResponse.setErrorCode("500");
                    errorResponse.setMessage("Error processing URL");
                    errorResponse.setTimestamp(LocalDateTime.now());
                    errorResponse.setDetails(throwable.getMessage());

                    // Return ResponseEntity with the ErrorResponse
                    return ResponseEntity.status(500).body(errorResponse);  // HTTP 500 Internal Server Error
                }
            });
    }

    @GetMapping(value = "/links", produces = MediaType.APPLICATION_JSON_VALUE)
    public CompletableFuture<PaginatedLinksResponse> getAllLinks(
            @RequestParam(required = false) String cursor,
            @RequestParam(defaultValue = "10") int limit) {

        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be greater than 0");
        }

        // Call the service method to get links asynchronously
        return urlService.getLinksAsync(limit, cursor);
    }
}
