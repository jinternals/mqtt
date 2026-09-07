package com.jinternals.mqtt.edge.config;

import com.jinternals.mqtt.edge.health.HealthPublisher;
import com.jinternals.mqtt.edge.protocol.RobotHealth;
import com.jinternals.mqtt.edge.protocol.Topics;
import com.jinternals.mqtt.spring.core.MqttWill;
import com.jinternals.mqtt.spring.support.MqttCodec;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * What is left of the MQTT wiring once the starter is doing the work.
 *
 * <p>The command subscription used to live here as an {@code MqttSubscription} bean, complete with
 * an {@code ObjectProvider} to break the cycle between the connection and the handler that needed
 * it. It is now a single {@code @MqttListener} annotation on the handler itself, and the cycle is
 * gone with it.
 *
 * <p>The Last Will stays declarative, because it is genuinely configuration rather than behaviour:
 * it is a message handed to the broker at connect time, not something this application ever runs.
 */
@Configuration(proxyBeanMethods = false)
public class EdgeMqttConfig {

    /**
     * The broker publishes this, retained, if our session ends without a clean DISCONNECT.
     *
     * <p>It has to be retained for the same reason the heartbeat is: it must <em>overwrite</em> the
     * retained UP message. A non-retained will would be delivered to whoever happens to be
     * subscribed at that moment and then vanish, leaving the retained UP in place — so anyone who
     * subscribed later would read a dead gateway as healthy.
     */
    @Bean
    public MqttWill gatewayLastWill(EdgeProperties props, MqttCodec codec) {
        RobotHealth will =
                RobotHealth.lastWill(props.getSiteId(), HealthPublisher.GATEWAY_ROBOT_ID);
        return new MqttWill(
                Topics.health(props.getSiteId(), HealthPublisher.GATEWAY_ROBOT_ID),
                codec.encode(will),
                1,
                true);
    }
}
