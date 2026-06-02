package org.fwp.route.stream.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.fwp.route.stream.model.Route;
import org.fwp.route.stream.properties.AppProperties;
import org.fwp.route.stream.serde.JsonSerde;
import org.fwp.route.stream.service.RouteService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Slf4j
@Component
@RequiredArgsConstructor
public class RouteTopology {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .findAndRegisterModules();

    private final AppProperties appProperties;
    private final RouteService routeService;

    @Autowired
    public void buildPipeline(StreamsBuilder builder) {
        String routesTopic = appProperties.getKafka().getTopics().getRoutes();
        String routesAgTopic = appProperties.getKafka().getTopics().getRoutesAg();

        Serde<Route> routeSerde = new JsonSerde<>(Route.class);

        KStream<String, String> rawRoutes = builder.stream(
                routesTopic,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withName("routes-source"));

        KStream<String, Route> parsed = rawRoutes
                .peek((k, v) -> log.debug("received key={} value={}", k, v), Named.as("incoming-routes"))
                .mapValues(this::parse, Named.as("parse-json"))
                .filter((k, v) -> v != null, Named.as("drop-bad-records"))
                // Enrichment: departureTime defaults to today @ 06:00 when absent in the payload.
                // Must run before keyOf() so the hash key is always stable and non-empty.
                .mapValues(RouteService::enrich, Named.as("enrich-departure-time"));

        // Re-key by deterministic identity hash, then materialize as a KTable.
        // Duplicate deliveries of the same logical route collapse to the same key.
        KTable<String, Route> routesTable = parsed
                .selectKey((k, route) -> RouteService.keyOf(route), Named.as("rekey-by-identity"))
                .repartition(Repartitioned.<String, Route>as("routes-rekeyed")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(routeSerde))
                .toTable(
                        Named.as("routes-table"),
                        Materialized.<String, Route, KeyValueStore<Bytes, byte[]>>as("routes-table-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(routeSerde));

        // Hold a reference to the filtered stream so we can fan out to two sinks.
        // foreach() is terminal (void) — chaining .to() after it does not compile.
        KStream<String, Route> changes = routesTable
                .toStream(Named.as("routes-table-changes"))
                .filter((k, v) -> v != null, Named.as("drop-tombstones"));

        // Sink #1 — DB upsert
        changes.foreach((id, route) -> routeService.upsert(route), Named.as("persist-route"));

        // Sink #2 — forward every live Route to routes-ag
        changes.to(routesAgTopic, Produced.with(Serdes.String(), routeSerde).withName("routes-ag-sink"));


    }

    private Route parse(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            return MAPPER.readValue(json, Route.class);
        } catch (Exception e) {
            log.warn("skipping unparseable route payload: {}", e.getMessage());
            return null;
        }
    }
}
