package com.jinternals.mqtt.spring.core;

import com.jinternals.mqtt.spring.annotation.MqttListener;
import com.jinternals.mqtt.spring.support.MqttCodec;

import java.nio.charset.StandardCharsets;
import org.springframework.util.Assert;

/**
 * The outbound half of the starter: serialise an object and publish it to a topic.
 *
 * <pre>{@code
 * gateway.send("sites/site1/telemetry/arm-01", reading);
 * gateway.send("sites/site1/health/arm-01", health, MqttSendOptions.retained());
 * gateway.send(topic, command, MqttSendOptions.expiringIn(Duration.ofMinutes(15)));
 * }</pre>
 *
 * <p>It exists because every publish site was otherwise repeating the same three steps — encode,
 * pick QoS, decide retain — and those are exactly the decisions that go quietly wrong. Pairing it
 * with {@link MqttListener} gives the starter a symmetric API: annotate to receive, inject this to
 * send.
 *
 * <h2>What "sent" means here</h2>
 *
 * {@code send} returns once the <b>local</b> broker has accepted the message, not when anything
 * downstream has consumed it. Over a store-and-forward topology that gap can be hours. Anything
 * needing confirmation of arrival needs an acknowledgement on a reply topic — the transport will
 * not tell you.
 *
 * <p>There is deliberately no retry: the local broker is a loopback hop, and a failure there is not
 * something a retry loop can fix. Durability across the unreliable hop is the broker's job, via its
 * persistent session and disk queue.
 */
public class MqttGateway {

    private final MqttConnection connection;
    private final MqttCodec codec;
    private final int defaultQos;

    public MqttGateway(MqttConnection connection, MqttCodec codec, int defaultQos) {
        this.connection = connection;
        this.codec = codec;
        this.defaultQos = defaultQos;
    }

    /** Publish with the configured defaults: {@code mqtt.qos}, not retained, no expiry. */
    public void send(String topic, Object payload) {
        send(topic, payload, MqttSendOptions.defaults());
    }

    /**
     * Publish with explicit options.
     *
     * @param payload a {@code byte[]} or {@code String} is sent as-is; anything else is encoded as
     *     JSON by {@link MqttCodec}
     * @throws MqttPublishException if the local broker rejects the message
     */
    public void send(String topic, Object payload, MqttSendOptions options) {
        Assert.hasText(topic, "topic must not be empty");
        Assert.notNull(payload, "payload must not be null");
        Assert.notNull(options, "options must not be null");

        connection.publish(
                topic,
                encode(payload),
                options.qos() != null ? options.qos() : defaultQos,
                options.retain(),
                options.expiry() == null ? null : options.expiry().toSeconds());
    }

    private byte[] encode(Object payload) {
        if (payload instanceof byte[] bytes) {
            return bytes;
        }
        if (payload instanceof String text) {
            return text.getBytes(StandardCharsets.UTF_8);
        }
        return codec.encode(payload);
    }

    /** Whether the local broker connection is currently up. */
    public boolean isConnected() {
        return connection.isConnected();
    }
}
