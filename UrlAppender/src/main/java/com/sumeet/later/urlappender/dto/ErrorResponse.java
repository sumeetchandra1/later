package com.sumeet.later.urlappender.dto;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class ErrorResponse {

    private String errorCode;
    private String message;
    private LocalDateTime timestamp;
    private String details;
}