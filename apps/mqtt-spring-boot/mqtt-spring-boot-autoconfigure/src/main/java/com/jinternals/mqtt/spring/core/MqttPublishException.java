package com.jinternals.mqtt.spring.core;

/**
 * Thrown when the local broker rejects a publish.
 *
 * <p>Rare, and not something a retry at the call site can fix: the local broker is a loopback hop,
 * so a failure here means the client is disconnected or its persistence store is full. Durability
 * across the unreliable hop is the broker's job, not the caller's.
 */
public class MqttPublishException extends RuntimeException {

    public MqttPublishException(String topic, Throwable cause) {
        super("Failed to publish to " + topic, cause);
    }
}
