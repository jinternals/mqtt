package com.jinternals.mqtt.cloud.listener;

import com.jinternals.mqtt.cloud.command.CommandDispatcher;
import com.jinternals.mqtt.cloud.fleet.FleetRegistry;
import com.jinternals.mqtt.cloud.protocol.CommandResponse;
import com.jinternals.mqtt.cloud.protocol.RobotHealth;
import com.jinternals.mqtt.cloud.protocol.Telemetry;
import com.jinternals.mqtt.cloud.protocol.Topics;
import com.jinternals.mqtt.spring.annotation.MqttListener;
import org.springframework.stereotype.Component;

/**
 * Everything the cloud consumes, in one place.
 *
 * <p>All four filters are wildcarded across sites, because adding site 3 must not require a code
 * change here — only a broker credential, an ACL block and a gateway.
 *
 * <p>This class replaced a configuration class that built four {@code MqttSubscription} beans by
 * hand, one of which needed an {@code ObjectProvider} to break a genuine cycle: the connection
 * needed the subscription list to be constructed, and the dispatcher that handled responses needed
 * the connection in order to publish. Discovery-by-annotation happens after every bean exists, so
 * the cycle simply does not arise.
 */
@Component
public class FleetListener {

    private final FleetRegistry registry;
    private final CommandDispatcher dispatcher;

    public FleetListener(FleetRegistry registry, CommandDispatcher dispatcher) {
        this.registry = registry;
        this.dispatcher = dispatcher;
    }

    @MqttListener(topic = Topics.ALL_TELEMETRY, qos = 1)
    public void onTelemetry(Telemetry telemetry) {
        registry.onTelemetry(telemetry);
    }

    @MqttListener(topic = Topics.ALL_HEALTH, qos = 1)
    public void onHealth(RobotHealth health) {
        registry.onHealth(health);
    }

    @MqttListener(topic = Topics.ALL_COMMAND_RESPONSES, qos = 1)
    public void onCommandResponse(CommandResponse response) {
        dispatcher.onResponse(response);
    }

    /**
     * Mosquitto's own bridge notifications, published by each site's broker. The payload is the
     * literal {@code "1"} or {@code "0"} rather than JSON, so this takes it as a raw String — the
     * cloud's fastest signal that a site's WAN link has changed state.
     */
    @MqttListener(topic = Topics.ALL_BRIDGE_STATE, qos = 1)
    public void onBridgeState(String payload, String topic) {
        String site = Topics.siteOf(topic);
        if (site != null) {
            registry.onBridgeState(site, payload);
        }
    }
}
