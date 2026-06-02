package org.fwp.route.stream.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "app")
public class AppProperties {

    private Kafka kafka = new Kafka();

    @Data
    public static class Kafka {
        private Topics topics = new Topics();

        @Data
        public static class Topics {
            /** Kafka topic consumed as the route event stream. */
            private String routes = "routes";
            /** Kafka topic produced with per-status route aggregates. */
            private String routesAg = "routes-ag";
        }
    }
}