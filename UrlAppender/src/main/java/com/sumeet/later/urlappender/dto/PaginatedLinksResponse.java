package com.sumeet.later.urlappender.dto;

import com.sumeet.later.urlappender.model.Link;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaginatedLinksResponse implements Serializable {

    private static final long serialVersionUID = 1L;  // Ensure serialization works correctly

    private List<Link> links;
    private String cursor;
}