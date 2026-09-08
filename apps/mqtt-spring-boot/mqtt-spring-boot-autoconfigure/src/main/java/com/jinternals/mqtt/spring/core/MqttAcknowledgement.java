package com.jinternals.mqtt.spring.core;

/**
 * A handle for acknowledging one received message.
 *
 * <p>Only meaningful when {@code mqtt.manual-acks=true}. Declare it as a parameter on an
 * {@code @MqttListener} method and the broker is not told the message was handled until you say so:
 *
 * <pre>{@code
 * @MqttListener(topic = "sites/+/telemetry/+")
 * void onTelemetry(Telemetry t, MqttAcknowledgement ack) {
 *     repository.save(t);   // durable somewhere else first
 *     ack.acknowledge();    // only now is the broker allowed to forget it
 * }
 * }</pre>
 *
 * <p>That ordering is the whole point: acknowledging before the data is safe somewhere else turns
 * at-least-once into at-most-once, silently.
 *
 * <p>Not acknowledging is a decision, not a leak. The broker holds the message and redelivers it
 * when the session next resumes — but it also counts against the inflight window, so a listener
 * that never acknowledges will eventually stall its own delivery. That backpressure is deliberate.
 */
@FunctionalInterface
public interface MqttAcknowledgement {

    /**
     * Tell the broker this message has been handled. Idempotent — calling it twice is harmless, so
     * a retry path does not have to track whether it already acknowledged.
     */
    void acknowledge();
}
