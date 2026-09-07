package com.jinternals.mqtt.edge.health;

import com.jinternals.mqtt.edge.config.EdgeProperties;
import com.jinternals.mqtt.edge.robot.RobotRegistry;
import com.jinternals.mqtt.edge.robot.RobotState;
import com.jinternals.mqtt.edge.telemetry.TelemetryPublisher;
import com.jinternals.mqtt.edge.protocol.RobotHealth;
import com.jinternals.mqtt.edge.protocol.HealthStatus;
import com.jinternals.mqtt.edge.protocol.Topics;
import com.jinternals.mqtt.spring.core.MqttGateway;
import com.jinternals.mqtt.spring.core.MqttSendOptions;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Heartbeats, published <b>retained</b> at QoS 1.
 *
 * <p>Retained matters more than it looks. Health is low-rate — one message every 15s — so without
 * the retain flag a cloud service that restarts is blind to a robot for up to a full interval,
 * and a robot that has gone quiet is indistinguishable from one that has never been seen. With
 * it, the broker replays the last known state to every new subscriber the instant it subscribes.
 *
 * <h2>Why the gateway heartbeat is special</h2>
 *
 * A Last Will is a property of an MQTT <em>connection</em>, and this gateway holds exactly one
 * connection on behalf of several robots. So the will can only cover the gateway itself
 * ({@code health/<site>/gateway}) — that one topic flips to DOWN automatically the moment the
 * process dies. Individual robot liveness has no such backstop and has to be inferred in the
 * cloud from a missed heartbeat, which is exactly what {@code FleetRegistry} does.
 */
@Component
public class HealthPublisher {

    /** Robot id the gateway publishes its own liveness under. Also the Last Will topic. */
    public static final String GATEWAY_ROBOT_ID = "gateway";

    private static final Logger log = LoggerFactory.getLogger(HealthPublisher.class);

    private final RobotRegistry registry;
    private final MqttGateway gateway;
    private final String siteId;
    private final Duration healthInterval;
    private final Instant startedAt = Instant.now();

    private volatile long nextDueAtMillis = 0;

    public HealthPublisher(
            RobotRegistry registry, MqttGateway gateway, EdgeProperties props) {
        this.registry = registry;
        this.gateway = gateway;
        this.siteId = props.getSiteId();
        this.healthInterval = props.getHealthInterval();
    }

    /**
     * A 1s tick with an explicit elapsed check, rather than {@code fixedDelayString} bound to the
     * property. {@code @Scheduled} resolves its interval once at startup from a String, so binding
     * it to a Duration property means either a brittle SpEL expression against a
     * {@code @ConfigurationProperties} bean name or a second millisecond-typed property. This is
     * the same pattern {@link TelemetryPublisher} uses and it keeps one source of truth.
     */
    @Scheduled(fixedDelay = 1000, initialDelay = 2000)
    public void tick() {
        long now = System.currentTimeMillis();
        if (now < nextDueAtMillis) {
            return;
        }
        nextDueAtMillis = now + healthInterval.toMillis();
        publishAll();
    }

    public void publishAll() {
        publishGatewayHealth();
        for (RobotState robot : registry.all()) {
            publishRobotHealth(robot);
        }
    }

    public void publishGatewayHealth() {
        Map<String, String> checks = new LinkedHashMap<>();
        checks.put("localBroker", gateway.isConnected() ? "ok" : "disconnected");
        checks.put("cells", String.valueOf(registry.all().size()));

        RobotHealth health =
                new RobotHealth(
                        siteId,
                        GATEWAY_ROBOT_ID,
                        gateway.isConnected() ? HealthStatus.UP : HealthStatus.DEGRADED,
                        Instant.now(),
                        Duration.between(startedAt, Instant.now()).toSeconds(),
                        null,
                        checks,
                        null);
        publish(GATEWAY_ROBOT_ID, health);
    }

    public void publishRobotHealth(RobotState robot) {
        Map<String, String> checks = new LinkedHashMap<>();
        checks.put("mode", robot.mode().name());
        checks.put("grasp", "ok");
        // A real cell reports its E-stop circuit here. It is the one check an operator looks at
        // first, so it belongs in the retained heartbeat rather than only in telemetry.
        checks.put("estop", "clear");
        checks.put("batteryPercent", String.valueOf(Math.round(robot.batteryPercent())));
        checks.put("intervalSeconds", String.valueOf(robot.interval().toSeconds()));

        RobotHealth health =
                new RobotHealth(
                        siteId,
                        robot.robotId(),
                        HealthStatus.UP,
                        Instant.now(),
                        Duration.between(startedAt, Instant.now()).toSeconds(),
                        robot.picksCompleted(),
                        checks,
                        null);
        publish(robot.robotId(), health);
    }

    private void publish(String robotId, RobotHealth health) {
        try {
            // Retained, and no message expiry: health is the last-known state of the fleet and
            // must never be discarded from a queue for being old.
            gateway.send(Topics.health(siteId, robotId), health, MqttSendOptions.retained());
        } catch (RuntimeException e) {
            log.error("Health publish failed for {}: {}", robotId, e.getMessage());
        }
    }
}
