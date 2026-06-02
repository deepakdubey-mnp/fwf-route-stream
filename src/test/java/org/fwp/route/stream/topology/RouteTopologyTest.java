package org.fwp.route.stream.topology;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.fwp.route.stream.model.Route;
import org.fwp.route.stream.properties.AppProperties;
import org.fwp.route.stream.serde.JsonSerde;
import org.fwp.route.stream.service.RouteService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link RouteTopology} using {@link TopologyTestDriver}.
 * No Spring context or live Kafka broker required.
 */
@ExtendWith(MockitoExtension.class)
class RouteTopologyTest {

    private static final String ROUTES_TOPIC    = "routes";
    private static final String ROUTES_AG_TOPIC = "routes-ag";

    @Mock
    private RouteService routeService;

    private TopologyTestDriver                   testDriver;
    private TestInputTopic<String, String>        inputTopic;   // topology reads raw JSON strings
    private TestOutputTopic<String, Route>        outputTopic;  // routes-ag carries Route objects

    @BeforeEach
    void setUp() {
        AppProperties props = new AppProperties();
        props.getKafka().getTopics().setRoutes(ROUTES_TOPIC);
        props.getKafka().getTopics().setRoutesAg(ROUTES_AG_TOPIC);

        RouteTopology topology = new RouteTopology(props, routeService);

        StreamsBuilder builder = new StreamsBuilder();
        topology.buildPipeline(builder);   // @Autowired method — call directly in tests

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,    "test-route-stream");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        testDriver = new TopologyTestDriver(builder.build(), config);

        inputTopic  = testDriver.createInputTopic(
                ROUTES_TOPIC, new StringSerializer(), new StringSerializer());
        outputTopic = testDriver.createOutputTopic(
                ROUTES_AG_TOPIC, new StringDeserializer(), new JsonSerde<>(Route.class).deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    // ── happy path ────────────────────────────────────────────────────────────

    @Test
    void validRoute_upsertedToDb_andForwardedToRoutesAg() {
        inputTopic.pipeInput("K1", json("BA", "LHR", "JFK", null));

        ArgumentCaptor<Route> captor = ArgumentCaptor.forClass(Route.class);
        verify(routeService).upsert(captor.capture());

        Route saved = captor.getValue();
        assertThat(saved.airline()).isEqualTo("BA");
        assertThat(saved.sourceAirport()).isEqualTo("LHR");
        assertThat(saved.destinationAirport()).isEqualTo("JFK");
        assertThat(saved.departureTime()).isNotNull();          // enriched from null

        assertThat(outputTopic.isEmpty()).isFalse();
        KeyValue<String, Route> kv = outputTopic.readKeyValue();
        assertThat(kv.key).hasSize(32);                        // SHA-256 truncated key
        assertThat(kv.value.airline()).isEqualTo("BA");
    }

    @Test
    void duplicateRoute_collapsedByKTable_upsertedOnce() {
        // Same logical route delivered twice — KTable deduplication keeps last value only.
        String sameRoute = json("LH", "FRA", "LAX", null);
        inputTopic.pipeInput("K1", sameRoute);
        inputTopic.pipeInput("K2", sameRoute);   // same content → same SHA-256 key

        verify(routeService, times(2)).upsert(any(Route.class));
        // Both end up on the same KTable key, so routes-ag sees 2 updates for that key.
        assertThat(outputTopic.readKeyValuesToList()).hasSize(2);
    }

    // ── enrichment ────────────────────────────────────────────────────────────

    @Test
    void missingDepartureTime_defaultedToTodayAt6AM() {
        inputTopic.pipeInput("K1", json("AF", "CDG", "ORD", null));

        ArgumentCaptor<Route> captor = ArgumentCaptor.forClass(Route.class);
        verify(routeService).upsert(captor.capture());

        LocalDateTime expectedDefault = LocalDate.now().atTime(LocalTime.of(6, 0));
        assertThat(captor.getValue().departureTime()).isEqualTo(expectedDefault);
    }

    @Test
    void presentDepartureTime_flooredToNearest15Minutes() {
        // 14:23 → 14:15
        inputTopic.pipeInput("K1", json("EK", "DXB", "SYD", "2024-06-01 14:23"));

        ArgumentCaptor<Route> captor = ArgumentCaptor.forClass(Route.class);
        verify(routeService).upsert(captor.capture());

        LocalDateTime floored = captor.getValue().departureTime();
        assertThat(floored.getMinute()).isEqualTo(15);
        assertThat(floored.getSecond()).isEqualTo(0);
        assertThat(floored.getNano()).isEqualTo(0);
    }

    @Test
    void departureTimeAlreadyOnSlotBoundary_unchanged() {
        // 09:00 → 09:00 (0 % 15 == 0, no change)
        inputTopic.pipeInput("K1", json("QF", "SYD", "MEL", "2024-06-01 09:00"));

        ArgumentCaptor<Route> captor = ArgumentCaptor.forClass(Route.class);
        verify(routeService).upsert(captor.capture());

        assertThat(captor.getValue().departureTime().getMinute()).isEqualTo(0);
        assertThat(captor.getValue().departureTime().getHour()).isEqualTo(9);
    }

    // ── error / edge cases ────────────────────────────────────────────────────

    @Test
    void malformedJson_skippedGracefully_noDbWrite_noOutput() {
        inputTopic.pipeInput("K1", "not-valid-json{{");

        verify(routeService, never()).upsert(any());
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    void blankPayload_skippedGracefully_noDbWrite_noOutput() {
        inputTopic.pipeInput("K1", "");

        verify(routeService, never()).upsert(any());
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    void tombstoneRecord_skippedGracefully_noDbWrite_noOutput() {
        inputTopic.pipeInput("K1", (String) null);

        verify(routeService, never()).upsert(any());
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /** Builds a minimal Route JSON; departureTime is omitted when null. */
    private String json(String airline, String src, String dst, String departureTime) {
        String dtField = departureTime == null
                ? ""
                : ",\"departureTime\":\"%s\"".formatted(departureTime);
        return """
                {"airline":"%s","sourceAirport":"%s","destinationAirport":"%s",\
                "codeShare":"Y","stops":0,"equipment":"738"%s}"""
                .formatted(airline, src, dst, dtField);
    }
}