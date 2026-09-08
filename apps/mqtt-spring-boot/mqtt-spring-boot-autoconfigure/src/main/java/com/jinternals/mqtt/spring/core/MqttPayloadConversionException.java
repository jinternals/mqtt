package com.jinternals.mqtt.spring.core;

/**
 * Raised when received bytes cannot be turned into the listener's payload type.
 *
 * <p>Distinct from any other handler failure because the correct response is the opposite one.
 * A handler that threw might succeed on redelivery — a network blip, a locked row — so withholding
 * the acknowledgement is right. A payload that will not parse will not parse next time either, so
 * withholding the acknowledgement just parks it in the inflight window until delivery stops.
 *
 * <p>So this is always acknowledged: dead-lettered when a dead-letter topic is configured, logged
 * and dropped when it is not.
 */
public class MqttPayloadConversionException extends RuntimeException {

    private final transient Class<?> targetType;

    public MqttPayloadConversionException(Class<?> targetType, Throwable cause) {
        super("Cannot decode payload as " + targetType.getSimpleName(), cause);
        this.targetType = targetType;
    }

    public Class<?> getTargetType() {
        return targetType;
    }
}
