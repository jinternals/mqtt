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
 * @param ackMode     who acknowledges; {@link MqttAckMode#INHERIT} defers to {@code mqtt.manual-acks}
 * @param handler     runs on the connection's dispatch thread
 */
public record MqttSubscription(
        String topicFilter, int qos, MqttAckMode ackMode, MqttMessageHandler handler) {

    /**
     * A subscription whose handler does not care about acknowledgement — the common case, where
     * {@code mqtt.manual-acks} is left at its default and the connection acknowledges once the
     * handler returns without throwing.
     */
    public static MqttSubscription atLeastOnce(String topicFilter, BiConsumer<String, byte[]> handler) {
        return new MqttSubscription(
                topicFilter, 1, MqttAckMode.INHERIT, (topic, payload, ack) -> handler.accept(topic, payload));
    }

    public static MqttSubscription atLeastOnce(String topicFilter, MqttMessageHandler handler) {
        return new MqttSubscription(topicFilter, 1, MqttAckMode.INHERIT, handler);
    }

    /**
     * Whether this subscription expects the listener to acknowledge, given the connection-wide
     * default that {@link MqttAckMode#INHERIT} defers to.
     */
    public boolean isManual(boolean connectionDefaultIsManual) {
        return switch (ackMode) {
            case MANUAL -> true;
            case AUTO -> false;
            case INHERIT -> connectionDefaultIsManual;
        };
    }
}
