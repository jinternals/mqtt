package com.jinternals.mqtt.spring.core;

import java.util.function.BiConsumer;

/**
 * A topic filter plus the code that handles it. Services declare these as beans; {@link
 * MqttConnection} picks them all up and (re)subscribes on every successful connect.
 *
 * @param topicFilter MQTT filter, may contain {@code +} and {@code #}
 * @param qos         subscription QoS — the delivery guarantee is the lower of publish and
 *                    subscribe QoS, so subscribing at 0 quietly downgrades everything
 * @param handler     receives (topic, payload). Runs on the connection's dispatch thread.
 */
public record MqttSubscription(String topicFilter, int qos, BiConsumer<String, byte[]> handler) {

    public static MqttSubscription atLeastOnce(String topicFilter, BiConsumer<String, byte[]> handler) {
        return new MqttSubscription(topicFilter, 1, handler);
    }
}
