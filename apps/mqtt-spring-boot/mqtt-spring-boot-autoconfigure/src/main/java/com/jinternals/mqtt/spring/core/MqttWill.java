package com.jinternals.mqtt.spring.core;

/**
 * The Last Will and Testament. The broker holds this from the moment we connect and publishes it
 * on our behalf if our session ends without a clean DISCONNECT — process killed, container OOM,
 * link dropped.
 *
 * <p>Declare it as a bean and {@link MqttConnection} installs it. If no bean is present the client
 * connects without a will, which is the right choice for a service whose disappearance is not
 * itself news (the cloud service, for instance).
 *
 * @param retained almost always {@code true} for a health topic: it must overwrite the retained
 *                 {@code UP} message, otherwise a late subscriber reads the stale success
 */
public record MqttWill(String topic, byte[] payload, int qos, boolean retained) {}
