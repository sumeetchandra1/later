package com.sumeet.later.urlappender.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import lombok.Data;

import java.util.Map;

@Data
public class AppendParametersRequest {

    @NotBlank(message = "ID cannot be blank")
    private String id;
    
    @NotBlank(message = "URL cannot be blank")
    @Pattern(regexp = "^(http|https)://.*$", message = "Invalid URL format")
    private String url;

    @NotEmpty(message = "Parameters cannot be empty")
    private Map<@NotBlank String, @NotBlank String> parameters;
}