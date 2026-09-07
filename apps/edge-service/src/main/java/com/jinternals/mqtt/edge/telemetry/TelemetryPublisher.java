package com.jinternals.mqtt.edge.telemetry;

import com.jinternals.mqtt.edge.config.EdgeProperties;
import com.jinternals.mqtt.edge.robot.RobotRegistry;
import com.jinternals.mqtt.edge.robot.RobotState;
import com.jinternals.mqtt.edge.protocol.Telemetry;
import com.jinternals.mqtt.edge.protocol.Topics;
import com.jinternals.mqtt.spring.core.MqttGateway;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Publishes readings to the <em>local</em> broker.
 *
 * <p>There is no retry, no backoff and no queue in this class, and that is the point. The publish
 * is a loopback call to a broker two containers away on the same site LAN; if it fails, something
 * is wrong locally and retrying will not help. Everything to do with the unreliable WAN link lives
 * in the bridge configuration, not here.
 *
 * <p>Telemetry is <b>not</b> retained. Retaining it would mean a new subscriber immediately
 * receives one stale reading per robot, which looks like live data and is not.
 */
@Component
public class TelemetryPublisher {

    private static final Logger log = LoggerFactory.getLogger(TelemetryPublisher.class);

    private final RobotRegistry registry;
    private final MqttGateway gateway;
    private final String siteId;

    public TelemetryPublisher(
            RobotRegistry registry, MqttGateway gateway, EdgeProperties props) {
        this.registry = registry;
        this.gateway = gateway;
        this.siteId = props.getSiteId();
    }

    /**
     * A 1s tick that publishes whichever robots are due, rather than one fixed-rate task per
     * robot — SET_INTERVAL can change a robot's cadence at runtime, and rescheduling Spring tasks
     * per command is a lot of machinery for a timer check.
     */
    @Scheduled(fixedDelay = 1000)
    public void tick() {
        long now = System.currentTimeMillis();
        for (RobotState robot : registry.all()) {
            if (robot.isDue(now)) {
                publish(robot);
                robot.markPublished(now);
            }
        }
    }

    public void publish(RobotState robot) {
        Telemetry telemetry =
                new Telemetry(
                        siteId,
                        robot.robotId(),
                        robot.nextSequence(),
                        Instant.now(),
                        robot.sample());
        try {
            // Default options: no retain, and crucially no expiry — a reading captured during a
            // six-hour outage is still the historical record and must arrive whenever the link
            // returns.
            gateway.send(Topics.telemetry(siteId, robot.robotId()), telemetry);
        } catch (RuntimeException e) {
            log.error("Local publish failed for {}: {}", robot.robotId(), e.getMessage());
        }
    }
}
