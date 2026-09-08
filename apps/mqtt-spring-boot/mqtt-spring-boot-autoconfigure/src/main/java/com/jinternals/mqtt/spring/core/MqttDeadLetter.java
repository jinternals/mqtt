package com.jinternals.mqtt.spring.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;

/**
 * What gets published to the dead-letter topic when a message cannot be handled.
 *
 * <p>Carries enough to diagnose and replay without going back to the logs: which topic it arrived
 * on, which subscription was handling it, what went wrong, and the payload itself.
 *
 * <p>The payload is kept as readable text whenever the bytes are valid UTF-8, because the first
 * thing anyone does with a dead-letter topic is point a topic browser at it. Binary payloads fall
 * back to base64 so nothing is ever silently mangled — exactly one of the two fields is set.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record MqttDeadLetter(
        String originalTopic,
        String subscription,
        String clientId,
        Instant failedAt,
        int attempts,
        Reason reason,
        String error,
        String payload,
        String payloadBase64) {

    public enum Reason {
        /** The listener threw, and retries (if any) were exhausted. */
        HANDLER_FAILED,
        /**
         * The bytes could not be turned into the listener's payload type. Retrying is pointless —
         * a malformed message is malformed on every redelivery.
         */
        PAYLOAD_UNDECODABLE
    }
}
