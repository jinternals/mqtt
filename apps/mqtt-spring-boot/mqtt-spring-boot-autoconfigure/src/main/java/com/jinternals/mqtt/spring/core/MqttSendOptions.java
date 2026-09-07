package com.jinternals.mqtt.spring.core;

import java.time.Duration;

/**
 * Per-message publish options.
 *
 * <p>Immutable with fluent withers, so a call site reads as a sentence and options can be shared as
 * constants without anyone mutating them:
 *
 * <pre>{@code
 * gateway.send(topic, reading);                                    // defaults
 * gateway.send(topic, health, MqttSendOptions.retained());         // last-known state
 * gateway.send(topic, command, MqttSendOptions.expiringIn(ttl));   // deadline
 * }</pre>
 *
 * <p>The component is {@code retain}, not {@code retained}, and that is deliberate rather than a
 * workaround: MQTT itself calls this "the RETAIN flag", so {@code options.retain()} reads as the
 * flag it is. It also leaves the past participle free for the factory, which is where the readable
 * name actually earns its keep — {@code MqttSendOptions.retained()} at a call site.
 *
 * @param qos    {@code null} uses the configured {@code mqtt.qos}
 * @param retain the MQTT RETAIN flag: the broker keeps this as the topic's last-known value
 * @param expiry MQTT 5 Message Expiry Interval; {@code null} means never expire. Use it only for
 *               messages that become <em>wrong</em> with age, such as commands — never for
 *               telemetry or health, whose whole value is that they survive an outage.
 */
public record MqttSendOptions(Integer qos, boolean retain, Duration expiry) {

    private static final MqttSendOptions DEFAULTS = new MqttSendOptions(null, false, null);

    /** Configured QoS, RETAIN clear, no expiry. */
    public static MqttSendOptions defaults() {
        return DEFAULTS;
    }

    /** RETAIN set: the broker keeps this as the topic's last-known value for new subscribers. */
    public static MqttSendOptions retained() {
        return DEFAULTS.withRetain();
    }

    /** Expiring: the broker discards it if it is still queued after {@code expiry}. */
    public static MqttSendOptions expiringIn(Duration expiry) {
        return DEFAULTS.withExpiry(expiry);
    }

    public MqttSendOptions withQos(int qos) {
        return new MqttSendOptions(qos, retain, expiry);
    }

    public MqttSendOptions withRetain() {
        return new MqttSendOptions(qos, true, expiry);
    }

    public MqttSendOptions withExpiry(Duration expiry) {
        return new MqttSendOptions(qos, retain, expiry);
    }
}
