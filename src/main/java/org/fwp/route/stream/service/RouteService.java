package org.fwp.route.stream.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.fwp.route.stream.entity.RouteEntity;
import org.fwp.route.stream.model.Route;
import org.fwp.route.stream.repository.RouteRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;

@Slf4j
@Service
@RequiredArgsConstructor
public class RouteService {

    private final RouteRepository routeRepository;

    @Transactional
    public RouteEntity upsert(Route route) {
        Route normalized = normalize(route);
        String id = keyOf(normalized);

        RouteEntity entity = routeRepository.findById(id)
                .map(existing -> apply(existing, normalized))
                .orElseGet(() -> toEntity(id, normalized));

        RouteEntity saved = routeRepository.save(entity);
        log.debug("upserted route id={} version={}", saved.getId(), saved.getVersion());
        return saved;
    }

    /**
     * Enrichment step — must be called on every incoming Route before any key derivation
     * or persistence.  If {@code departureTime} is absent (null), it is defaulted to
     * today at 06:00 so the field is always meaningful downstream.
     */
    public static Route enrich(Route route) {
        LocalDateTime enriched;
        if (route.departureTime() == null) {
            enriched = LocalDate.now().atTime(LocalTime.of(6, 0));
            log.debug("departureTime missing — defaulting to {} for airline={} {}→{}",
                    enriched, route.airline(), route.sourceAirport(), route.destinationAirport());
        } else {
            // Floor to nearest 15-minute slot (e.g. 14:23 → 14:15, 09:03 → 09:00)
            LocalDateTime t = route.departureTime();
            enriched = t.withMinute(t.getMinute() - (t.getMinute() % 15))
                        .withSecond(0)
                        .withNano(0);
            log.debug("departureTime {} floored to {} for airline={} {}→{}",
                    t, enriched, route.airline(), route.sourceAirport(), route.destinationAirport());
        }
        return new Route(
                route.airline(),
                route.sourceAirport(),
                route.destinationAirport(),
                route.codeShare(),
                route.stops(),
                route.equipment(),
                enriched);
    }

    private static Route normalize(Route route) {
        return new Route(
                route.airline(),
                upper(route.sourceAirport()),
                upper(route.destinationAirport()),
                route.codeShare(),
                route.stops(),
                route.equipment(),
                route.departureTime());
    }

    private static String upper(String s) {
        return s == null ? null : s.toUpperCase(Locale.ROOT);
    }

    @Transactional
    public List<RouteEntity> upsertAll(List<Route> routes) {
        return routes.stream().map(this::upsert).toList();
    }

    /**
     * Stable, content-derived identity. Same airline + source + destination + departureTime
     * always produces the same id, so re-delivery of the same flight upserts in place.
     * Fields not in the key (codeShare, equipment, stops) are treated as mutable attributes.
     */
    public static String keyOf(Route route) {
        String canonical = String.join("|",
                nullSafe(route.airline()),
                nullSafe(upper(route.sourceAirport())),
                nullSafe(upper(route.destinationAirport())),
                nullSafe(route.departureTime()));
        return sha256Hex(canonical);
    }

    private static String nullSafe(String s) {
        return s == null ? "" : s;
    }

    private static String nullSafe(LocalDateTime t) {
        return t == null ? "" : t.toString();
    }

    private static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest).substring(0, 32);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static RouteEntity toEntity(String id, Route route) {
        return RouteEntity.builder()
                .id(id)
                .airline(route.airline())
                .sourceAirport(route.sourceAirport())
                .destinationAirport(route.destinationAirport())
                .codeShare(route.codeShare())
                .stops(route.stops())
                .equipment(route.equipment())
                .departureTime(route.departureTime())
                .build();
    }

    private static RouteEntity apply(RouteEntity entity, Route route) {
        entity.setCodeShare(route.codeShare());
        entity.setStops(route.stops());
        entity.setEquipment(route.equipment());
        return entity;
    }
}
