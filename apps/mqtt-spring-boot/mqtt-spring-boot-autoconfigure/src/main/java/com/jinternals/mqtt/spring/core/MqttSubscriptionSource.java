package com.jinternals.mqtt.spring.core;

import java.util.List;

/**
 * Somewhere subscriptions come from, resolved when the connection starts rather than when it is
 * built.
 *
 * <p>Exists to keep the dependency pointing one way. {@link MqttConnection} needs whatever the
 * annotation scan discovered, but {@code core} must not depend on the scanning package — that would
 * be a cycle, and it would stop {@code core} being usable on its own. So core states the contract
 * and the listener package implements it.
 *
 * <p>Resolution is deferred because discovery happens while beans are initialising, which can be
 * after the connection bean is created but is always before the lifecycle processor starts it.
 */
@FunctionalInterface
public interface MqttSubscriptionSource {

    /** Empty when nothing was discovered. Never {@code null}. */
    List<MqttSubscription> subscriptions();
}
