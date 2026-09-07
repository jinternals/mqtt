package com.jinternals.mqtt.spring.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;

/**
 * JSON on the wire.
 *
 * <p>JSON is the wrong choice for a genuinely constrained link — CBOR or protobuf would cut this
 * payload by more than half — but it is the right choice for a demo you have to debug with
 * {@code mosquitto_sub}. The codec is isolated here so swapping it is a one-file change.
 */
public class MqttCodec {

    private final ObjectMapper mapper;

    public MqttCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public byte[] encode(Object value) {
        try {
            return mapper.writeValueAsBytes(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot serialise " + value.getClass().getName(), e);
        }
    }

    /**
     * Returns {@code null} on malformed input rather than throwing. A single bad payload — a
     * half-written retained message, an old schema replayed from a bridge backlog — must not take
     * out the dispatch loop for every other device.
     */
    public <T> T decode(byte[] payload, Class<T> type) {
        try {
            return mapper.readValue(payload, type);
        } catch (Exception e) {
            return null;
        }
    }

    public String asString(byte[] payload) {
        return new String(payload, StandardCharsets.UTF_8);
    }
}
