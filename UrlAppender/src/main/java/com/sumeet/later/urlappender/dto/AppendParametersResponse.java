package com.sumeet.later.urlappender.dto;

import lombok.Data;

import java.util.Map;

@Data
public class AppendParametersResponse {

    private String originalUrl;
    private Map<String, String> parameters;
    private String newUrl;
}