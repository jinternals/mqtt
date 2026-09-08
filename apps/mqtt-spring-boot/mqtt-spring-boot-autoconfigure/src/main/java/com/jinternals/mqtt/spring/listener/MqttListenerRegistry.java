package com.jinternals.mqtt.spring.listener;

import com.jinternals.mqtt.spring.annotation.MqttListener;
import com.jinternals.mqtt.spring.core.MqttConnection;
import com.jinternals.mqtt.spring.core.MqttSubscription;
import com.jinternals.mqtt.spring.core.MqttSubscriptionSource;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Collects subscriptions discovered from {@link MqttListener} methods.
 *
 * <p>Exists to decouple discovery from connection. {@link MqttListenerAnnotationBeanPostProcessor}
 * writes here as beans are initialised; {@link MqttConnection} reads it in {@code start()}, which
 * the lifecycle processor runs only after every singleton exists. That ordering is what lets a
 * listener be declared on any bean without caring whether it happens to be created before or after
 * the connection.
 *
 * <p>Has no dependencies of its own, deliberately: it is instantiated early alongside the bean
 * post-processor, and anything it injected would be dragged into early initialisation with it.
 */
public class MqttListenerRegistry implements MqttSubscriptionSource {

    private final List<MqttSubscription> discovered = new CopyOnWriteArrayList<>();

    public void register(MqttSubscription subscription) {
        discovered.add(subscription);
    }

    public List<MqttSubscription> all() {
        return List.copyOf(discovered);
    }

    @Override
    public List<MqttSubscription> subscriptions() {
        return all();
    }

    public int size() {
        return discovered.size();
    }
}
