package com.jinternals.mqtt.spring.core;

/**
 * Receives one message for a subscription.
 *
 * <p>Carries the acknowledgement handle alongside the payload rather than acknowledging behind the
 * handler's back, because <em>when</em> a message is acknowledged is a correctness decision that
 * belongs to whoever knows when the work is actually durable.
 */
@FunctionalInterface
public interface MqttMessageHandler {

    /**
     * @param topic the concrete topic the message arrived on — how you recover what {@code +} matched
     * @param payload raw bytes, exactly as published
     * @param acknowledgement use it when {@code mqtt.manual-acks=true}; ignored otherwise, because
     *     the connection acknowledges automatically once this method returns without throwing
     */
    void handle(String topic, byte[] payload, MqttAcknowledgement acknowledgement);
}
