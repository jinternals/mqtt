package com.jinternals.mqtt.spring.annotation;

import com.jinternals.mqtt.spring.support.MqttCodec;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as the handler for an MQTT topic filter.
 *
 * <pre>{@code
 * @Component
 * class CommandListener {
 *
 *     @MqttListener(topic = "sites/${app.site-id}/command/+/req")
 *     void onCommand(CommandRequest request, String topic) {
 *         ...
 *     }
 * }
 * }</pre>
 *
 * <h2>Method signature</h2>
 *
 * The rule is positional and deliberately boring, because clever parameter resolution is the part
 * of these annotations that people end up debugging:
 *
 * <ul>
 *   <li><b>First parameter — the payload.</b> {@code byte[]} gets the raw bytes, {@code String}
 *       gets it as UTF-8, anything else is decoded from JSON by {@link MqttCodec}.
 *   <li><b>Optional second parameter — the topic.</b> Must be {@code String}. It is the concrete
 *       topic the message arrived on, which is how you recover the values behind {@code +}.
 * </ul>
 *
 * <p>A payload that fails to decode is logged and dropped, not thrown: one malformed message — a
 * half-written retained value, an old schema replayed out of a bridge backlog — must not take down
 * delivery for every other device.
 *
 * <p>{@code topic} is resolved against the {@code Environment}, so {@code ${...}} placeholders
 * work and a per-instance topic can come from configuration.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MqttListener {

    /** Topic filter. May contain {@code +} and {@code #}, and {@code ${...}} placeholders. */
    String topic();

    /**
     * Subscription QoS. Delivery is the <em>lower</em> of publish and subscribe QoS, so leaving
     * this at 0 quietly downgrades an at-least-once publisher.
     */
    int qos() default 1;
}
