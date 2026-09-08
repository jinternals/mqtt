package com.jinternals.mqtt.spring.core;

/** Who acknowledges a message once a listener has seen it. */
public enum MqttAckMode {

    /** Take {@code mqtt.manual-acks}. The default, so one setting still governs a whole service. */
    INHERIT,

    /**
     * The connection acknowledges once the listener returns without throwing. The listener must not
     * declare an {@link MqttAcknowledgement} parameter — acknowledging early and then throwing would
     * lose the message.
     */
    AUTO,

    /**
     * The listener acknowledges, via an {@link MqttAcknowledgement} parameter. For work that is only
     * really "handled" once it is durable somewhere else.
     */
    MANUAL
}
