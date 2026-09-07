package com.jinternals.mqtt.cloud.fleet;

import com.jinternals.mqtt.cloud.config.CloudProperties;
import com.jinternals.mqtt.cloud.protocol.RobotHealth;
import com.jinternals.mqtt.cloud.protocol.HealthStatus;
import com.jinternals.mqtt.cloud.protocol.Telemetry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Last-known state of every robot across every site, kept in memory.
 *
 * <p>In production this is a time-series database plus a robot table; in-memory keeps the demo to
 * one moving part. What is <em>not</em> simplified is how arrival is interpreted, because that is
 * where store-and-forward systems get subtly wrong:
 *
 * <ul>
 *   <li><b>Freshness is judged on {@code capturedAt}, never on arrival.</b> When a site's bridge
 *       reconnects it dumps hours of backlog in seconds. Every one of those messages arrives
 *       "now"; almost none of them describe now.
 *   <li><b>Out-of-order and replayed messages are rejected by sequence number.</b> A redelivered
 *       QoS 1 message must not overwrite newer state with older readings.
 *   <li><b>A gap in sequence numbers is recorded.</b> QoS 1 tells you a message was delivered; only
 *       the sequence tells you one never was.
 * </ul>
 */
@Component
public class FleetRegistry {

    private static final Logger log = LoggerFactory.getLogger(FleetRegistry.class);

    private final Map<String, RobotView> robots = new ConcurrentHashMap<>();
    private final Map<String, BridgeView> bridges = new ConcurrentSkipListMap<>();
    private final Duration healthTimeout;
    private final Duration backlogLagThreshold;

    private final Counter lateArrivals;
    private final Counter outOfOrder;
    private final Counter sequenceGaps;

    public FleetRegistry(CloudProperties props, MeterRegistry meters) {
        this.healthTimeout = props.getHealthTimeout();
        this.backlogLagThreshold = props.getBacklogLagThreshold();
        this.lateArrivals = Counter.builder("cloud.telemetry.late").register(meters);
        this.outOfOrder = Counter.builder("cloud.telemetry.out_of_order").register(meters);
        this.sequenceGaps = Counter.builder("cloud.telemetry.sequence_gaps").register(meters);
        meters.gauge("cloud.robots.tracked", robots, Map::size);
    }

    // ------------------------------------------------------------------ ingest

    public void onTelemetry(Telemetry telemetry) {
        String key = key(telemetry.site(), telemetry.robotId());
        Instant arrivedAt = Instant.now();

        robots.compute(
                key,
                (k, existing) -> {
                    RobotView view =
                            existing != null
                                    ? existing
                                    : new RobotView(telemetry.site(), telemetry.robotId());

                    if (telemetry.sequence() <= view.lastSequence) {
                        // Replay or reorder. Count it and keep the newer state we already have.
                        outOfOrder.increment();
                        view.duplicateOrReordered++;
                        return view;
                    }
                    if (view.lastSequence > 0 && telemetry.sequence() > view.lastSequence + 1) {
                        long missed = telemetry.sequence() - view.lastSequence - 1;
                        sequenceGaps.increment(missed);
                        view.missedMessages += missed;
                        log.warn("Gap of {} message(s) for {} (seq {} -> {})",
                                missed, key, view.lastSequence, telemetry.sequence());
                    }

                    long lagMillis = Duration.between(telemetry.capturedAt(), arrivedAt).toMillis();
                    if (lagMillis > backlogLagThreshold.toMillis()) {
                        // Almost certainly drained from a bridge backlog after an outage.
                        lateArrivals.increment();
                        view.backloggedMessages++;
                    }

                    view.lastSequence = telemetry.sequence();
                    view.lastTelemetry = telemetry;
                    view.lastCapturedAt = telemetry.capturedAt();
                    view.lastArrivedAt = arrivedAt;
                    view.maxIngestLagMillis = Math.max(view.maxIngestLagMillis, lagMillis);
                    view.messagesReceived++;
                    return view;
                });
    }

    public void onHealth(RobotHealth health) {
        String key = key(health.site(), health.robotId());
        robots.compute(
                key,
                (k, existing) -> {
                    RobotView view =
                            existing != null
                                    ? existing
                                    : new RobotView(health.site(), health.robotId());
                    view.lastHealth = health;
                    // A Last Will has no reportedAt (the broker sent it, the robot did not), so
                    // fall back to arrival time for the freshness clock.
                    view.lastHealthAt =
                            health.reportedAt() != null ? health.reportedAt() : Instant.now();
                    if (health.status() == HealthStatus.DOWN) {
                        log.warn("{} reported DOWN: {}", key, health.detail());
                    }
                    return view;
                });
    }

    /**
     * Mosquitto's bridge notification: the literal payload {@code "1"} means the site's link to
     * this broker is up, {@code "0"} means it is down. This is the cloud's only direct signal about
     * WAN health, and it is far faster than waiting for heartbeats to go stale.
     */
    public void onBridgeState(String site, String payload) {
        boolean connected = "1".equals(payload.trim());
        bridges.compute(
                site,
                (s, existing) -> {
                    BridgeView view = existing != null ? existing : new BridgeView(site);
                    if (view.connected != null && view.connected != connected) {
                        view.transitions++;
                    }
                    view.connected = connected;
                    view.changedAt = Instant.now();
                    return view;
                });
        log.warn("Bridge for site {} is now {}", site, connected ? "UP" : "DOWN");
    }

    // ------------------------------------------------------------------ query

    public List<RobotSnapshot> snapshot() {
        Instant now = Instant.now();
        List<RobotSnapshot> out = new ArrayList<>(robots.size());
        for (RobotView v : robots.values()) {
            out.add(v.toSnapshot(now, healthTimeout));
        }
        out.sort(Comparator.comparing(RobotSnapshot::site).thenComparing(RobotSnapshot::robotId));
        return out;
    }

    public List<BridgeSnapshot> bridgeSnapshot() {
        List<BridgeSnapshot> out = new ArrayList<>();
        for (BridgeView v : bridges.values()) {
            out.add(new BridgeSnapshot(v.site, v.connected, v.changedAt, v.transitions));
        }
        return out;
    }

    private static String key(String site, String robotId) {
        return site + "/" + robotId;
    }

    // ------------------------------------------------------------------ state

    private static final class RobotView {
        final String site;
        final String robotId;
        long lastSequence;
        long messagesReceived;
        long missedMessages;
        long duplicateOrReordered;
        long backloggedMessages;
        long maxIngestLagMillis;
        Telemetry lastTelemetry;
        Instant lastCapturedAt;
        Instant lastArrivedAt;
        RobotHealth lastHealth;
        Instant lastHealthAt;

        RobotView(String site, String robotId) {
            this.site = site;
            this.robotId = robotId;
        }

        RobotSnapshot toSnapshot(Instant now, Duration timeout) {
            HealthStatus status;
            if (lastHealth != null && lastHealth.status() == HealthStatus.DOWN) {
                // An explicit Last Will beats any freshness heuristic.
                status = HealthStatus.DOWN;
            } else if (lastHealthAt == null || Duration.between(lastHealthAt, now).compareTo(timeout) > 0) {
                status = null; // rendered as STALE
            } else {
                status = lastHealth != null ? lastHealth.status() : HealthStatus.UP;
            }
            return new RobotSnapshot(
                    site,
                    robotId,
                    status == null ? "STALE" : status.name(),
                    lastCapturedAt,
                    lastArrivedAt,
                    lastHealthAt,
                    lastSequence,
                    messagesReceived,
                    missedMessages,
                    duplicateOrReordered,
                    backloggedMessages,
                    maxIngestLagMillis,
                    lastTelemetry == null ? null : lastTelemetry.metrics(),
                    lastHealth == null ? null : lastHealth.checks());
        }
    }

    private static final class BridgeView {
        final String site;
        Boolean connected;
        Instant changedAt;
        long transitions;

        BridgeView(String site) {
            this.site = site;
        }
    }

    public record RobotSnapshot(
            String site,
            String robotId,
            String status,
            Instant lastCapturedAt,
            Instant lastArrivedAt,
            Instant lastHealthAt,
            long lastSequence,
            long messagesReceived,
            long missedMessages,
            long duplicateOrReordered,
            long backloggedMessages,
            long maxIngestLagMillis,
            Map<String, Double> lastMetrics,
            Map<String, String> lastChecks) {}

    public record BridgeSnapshot(String site, Boolean connected, Instant changedAt, long transitions) {}
}
