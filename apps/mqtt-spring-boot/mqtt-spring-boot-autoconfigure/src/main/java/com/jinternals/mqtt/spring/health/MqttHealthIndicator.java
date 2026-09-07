package com.jinternals.mqtt.spring.health;

import com.jinternals.mqtt.spring.core.MqttConnection;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Surfaces the broker connection on {@code /actuator/health}.
 *
 * <p>Reported as DOWN when disconnected, which is what the container healthcheck and any load
 * balancer key off. Note the deliberate asymmetry with the <em>bridge</em>: an edge service whose
 * site link is down is still perfectly healthy — it is publishing to its local broker and the
 * bridge is spooling. Only losing the <em>local</em> broker is an application-level outage.
 */
public class MqttHealthIndicator implements HealthIndicator {

    private final MqttConnection connection;

    public MqttHealthIndicator(MqttConnection connection) {
        this.connection = connection;
    }

    @Override
    public Health health() {
        Health.Builder builder = connection.isConnected() ? Health.up() : Health.down();
        return builder.withDetail("broker", connection.getServerUri())
                .withDetail("clientId", connection.getClientId())
                .withDetail("disconnects", connection.getDisconnectCount())
                .withDetail("lastConnectedAt", connection.getLastConnectedAt())
                .build();
    }
}
