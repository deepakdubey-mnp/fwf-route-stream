package org.fwp.route.stream.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Kafka Streams wiring for Spring Boot 4.x.
 *
 * Spring Boot 4 removed KafkaStreamsAutoConfiguration, so the required
 * 'defaultKafkaStreamsConfig' bean must be declared explicitly.  All tunables
 * (bootstrap servers, state dir, serdes, acks, compression, security, etc.)
 * are still read from application[-profile].properties — nothing is hardcoded.
 */
@Slf4j
@Configuration
@EnableKafkaStreams
public class KafkaConfig {

    private static final String STREAMS_PROPS_PREFIX = "spring.kafka.streams.properties.";

    /**
     * Required by {@code @EnableKafkaStreams} — bean name must be exactly
     * {@value KafkaStreamsDefaultConfiguration#DEFAULT_STREAMS_CONFIG_BEAN_NAME}.
     *
     * Reads three groups of properties from the active Spring profile:
     * <ol>
     *   <li>{@code spring.kafka.bootstrap-servers} — broker address(es)</li>
     *   <li>{@code spring.kafka.streams.application-id} and {@code state-dir}</li>
     *   <li>All {@code spring.kafka.streams.properties.*} — serdes, threads, acks,
     *       compression, min.insync.replicas, SASL/SSL security, etc.</li>
     * </ol>
     *
     * Using {@link EnumerablePropertySource} iteration instead of
     * {@code KafkaProperties} avoids a compile-time dependency on
     * {@code spring-boot-autoconfigure} and works with any property source
     * (files, environment variables, AWS Parameter Store, etc.).
     */
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfig(ConfigurableEnvironment env) {
        Map<String, Object> props = new LinkedHashMap<>();

        // ── required ────────────────────────────────────────────────────────
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                env.getRequiredProperty("spring.kafka.bootstrap-servers"));
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                env.getRequiredProperty("spring.kafka.streams.application-id"));

        // ── optional core ────────────────────────────────────────────────────
        String stateDir = env.getProperty("spring.kafka.streams.state-dir");
        if (stateDir != null) {
            props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        }

        // ── all spring.kafka.streams.properties.* ────────────────────────────
        // Keys retain their dots (e.g. "producer.acks", "topic.min.insync.replicas",
        // "default.key.serde", "sasl.jaas.config") so they are forwarded verbatim
        // to Kafka Streams / its embedded producer and consumer clients.
        props.putAll(prefixedProperties(env, STREAMS_PROPS_PREFIX));

        log.info("KafkaStreamsConfiguration built: appId={} brokers={}",
                props.get(StreamsConfig.APPLICATION_ID_CONFIG),
                props.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));

        return new KafkaStreamsConfiguration(props);
    }

    /**
     * Extracts all properties whose key starts with {@code prefix}, stripping
     * the prefix from each key in the result.  Iterates every
     * {@link EnumerablePropertySource} in declaration order so profile-specific
     * sources (application-local.properties, application-ec2.properties) take
     * precedence over the base application.properties, matching standard Spring
     * profile override semantics.
     */
    private static Map<String, Object> prefixedProperties(
            ConfigurableEnvironment env, String prefix) {
        Map<String, Object> result = new LinkedHashMap<>();
        env.getPropertySources().stream()
                .filter(ps -> ps instanceof EnumerablePropertySource<?>)
                .flatMap(ps -> Arrays.stream(((EnumerablePropertySource<?>) ps).getPropertyNames()))
                .filter(name -> name.startsWith(prefix))
                .distinct()
                .forEach(name -> result.putIfAbsent(
                        name.substring(prefix.length()),
                        env.getProperty(name)));
        return result;
    }

    /**
     * Replaces the failed stream thread instead of shutting the whole application down.
     */
    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsCustomizer() {
        return factoryBean -> factoryBean.setStreamsUncaughtExceptionHandler(ex -> {
            log.error("Uncaught exception in Kafka Streams thread: {}", ex.getMessage(), ex);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
    }
}