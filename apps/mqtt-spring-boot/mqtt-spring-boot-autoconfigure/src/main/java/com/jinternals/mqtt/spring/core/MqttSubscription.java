package com.jinternals.mqtt.spring.core;

import java.util.function.BiConsumer;

/**
 * A topic filter plus the code that handles it. Services declare these as beans, or declare
 * {@code @MqttListener} methods and let the starter build them; {@link MqttConnection} picks up both
 * and (re)subscribes on every successful connect.
 *
 * @param topicFilter MQTT filter, may contain {@code +} and {@code #}
 * @param qos         subscription QoS — the delivery guarantee is the lower of publish and
 *                    subscribe QoS, so subscribing at 0 quietly downgrades everything
 * @param handler     runs on the connection's dispatch thread
 */
public record MqttSubscription(String topicFilter, int qos, MqttMessageHandler handler) {

    /**
     * A subscription whose handler does not care about acknowledgement — the common case, where
     * {@code mqtt.manual-acks} is left at its default and the connection acknowledges once the
     * handler returns without throwing.
     */
    public static MqttSubscription atLeastOnce(String topicFilter, BiConsumer<String, byte[]> handler) {
        return new MqttSubscription(topicFilter, 1, (topic, payload, ack) -> handler.accept(topic, payload));
    }

    public static MqttSubscription atLeastOnce(String topicFilter, MqttMessageHandler handler) {
        return new MqttSubscription(topicFilter, 1, handler);
    }
}
