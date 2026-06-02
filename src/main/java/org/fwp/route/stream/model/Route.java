package org.fwp.route.stream.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.time.LocalDateTime;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record Route(
        String airline,
        String sourceAirport,
        String destinationAirport,
        String codeShare,
        int stops,
        String equipment,
        @JsonFormat(pattern = "yyyy-MM-dd HH:mm") LocalDateTime departureTime
) implements Serializable {}