package org.fwp.route.stream.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

/**
 * Kafka message payload produced to the {@code routes-ag} topic.
 * One record per group key (currently airline) per tumbling window.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RouteAggregate {

    private String status;
    private long count;
    private long totalStops;
    private Set<String> airlines;
    private Set<String> sourceAirports;
    private Set<String> destinationAirports;
    private Set<String> equipments;
    private LocalDateTime windowStart;
    private LocalDateTime windowEnd;

    public RouteAggregate add(Route route) {
        if (airlines == null) airlines = new HashSet<>();
        if (sourceAirports == null) sourceAirports = new HashSet<>();
        if (destinationAirports == null) destinationAirports = new HashSet<>();
        if (equipments == null) equipments = new HashSet<>();

        count++;
        totalStops += route.stops();
        if (route.airline() != null) airlines.add(route.airline());
        if (route.sourceAirport() != null) sourceAirports.add(route.sourceAirport());
        if (route.destinationAirport() != null) destinationAirports.add(route.destinationAirport());
        if (route.equipment() != null) equipments.add(route.equipment());

        LocalDateTime ts = route.departureTime();
        if (ts != null) {
            if (windowStart == null || ts.isBefore(windowStart)) windowStart = ts;
            if (windowEnd == null || ts.isAfter(windowEnd)) windowEnd = ts;
        }
        return this;
    }
}
