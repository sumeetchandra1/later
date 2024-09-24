package com.sumeet.later.urlappender.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;


import java.io.Serializable;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@DynamoDbBean
public class Link implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private String originalUrl;
    private Map<String, String> parameters;
    private String newUrl;
    private Long creationTimestamp;


    @DynamoDbPartitionKey
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @DynamoDbAttribute("OriginalUrl")
    public String getOriginalUrl() {
        return originalUrl;
    }

    public void setOriginalUrl(String originalUrl) {
        this.originalUrl = originalUrl;
    }

    @DynamoDbAttribute("Parameters")
    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    @DynamoDbAttribute("NewUrl")
    public String getNewUrl() {
        return newUrl;
    }

    public void setNewUrl(String newUrl) {
        this.newUrl = newUrl;
    }

    @DynamoDbAttribute("creationTimestamp") // Map to DynamoDB attribute
    public Long getCreationTimestamp() {
        return creationTimestamp;
    }
    public void setCreationTimestamp(Long creationTimestamp) {
        this.creationTimestamp = creationTimestamp;
    }
}