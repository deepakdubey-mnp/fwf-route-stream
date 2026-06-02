package org.fwp.route.stream.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

/**
 * Generic Jackson-based JSON Serde for Kafka Streams.
 * One instance per type; thread-safe because ObjectMapper is shared and immutable after config.
 */
public class JsonSerde<T> implements Serde<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    private final Class<T> targetType;

    public JsonSerde(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing " + targetType.getSimpleName() + " to JSON", e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.readValue(data, targetType);
            } catch (IOException e) {
                throw new SerializationException("Error deserializing JSON to " + targetType.getSimpleName(), e);
            }
        };
    }
}