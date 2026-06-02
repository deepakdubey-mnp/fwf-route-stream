package org.fwp.fwproutestream;

import org.fwp.route.stream.FwpRouteStreamApplication;
import org.junit.jupiter.api.Test;

/**
 * Integration smoke test intentionally kept lightweight.
 * Full context load requires a running PostgreSQL + Kafka broker.
 * Use RouteTopologyTest for topology unit tests (no infrastructure needed).
 */
class FwpRouteStreamApplicationTests {

    @Test
    void applicationClassExists() {
        // Confirms the class compiles and is on the classpath.
        Class<?> clazz = FwpRouteStreamApplication.class;
        assert clazz != null;
    }
}