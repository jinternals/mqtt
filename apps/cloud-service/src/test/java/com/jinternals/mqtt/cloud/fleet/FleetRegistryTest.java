package com.jinternals.mqtt.cloud.fleet;

import com.jinternals.mqtt.cloud.config.CloudProperties;
import static org.assertj.core.api.Assertions.assertThat;

import com.jinternals.mqtt.cloud.protocol.RobotHealth;
import com.jinternals.mqtt.cloud.protocol.HealthStatus;
import com.jinternals.mqtt.cloud.protocol.Telemetry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The ingest rules that only matter because the link is unreliable: replays must not overwrite
 * newer state, gaps must be counted, and a Last Will must beat any freshness heuristic.
 */
class FleetRegistryTest {

    private FleetRegistry registry;

    @BeforeEach
    void setUp() {
        CloudProperties props = new CloudProperties();
        props.setHealthTimeout(Duration.ofSeconds(60));
        props.setBacklogLagThreshold(Duration.ofSeconds(30));
        registry = new FleetRegistry(props, new SimpleMeterRegistry());
    }

    private Telemetry telemetry(long sequence, Instant capturedAt) {
        return new Telemetry("site1", "arm-01", sequence, capturedAt, Map.of("picksPerMinute", 12.4));
    }

    private FleetRegistry.RobotSnapshot only() {
        return registry.snapshot().get(0);
    }

    @Test
    @DisplayName("a redelivered message does not overwrite newer state")
    void replayIsIgnored() {
        Instant now = Instant.now();
        registry.onTelemetry(telemetry(1, now.minusSeconds(10)));
        registry.onTelemetry(telemetry(2, now));
        registry.onTelemetry(telemetry(2, now)); // QoS 1 redelivery after a bridge reconnect

        assertThat(only().lastSequence()).isEqualTo(2);
        assertThat(only().messagesReceived()).isEqualTo(2);
        assertThat(only().duplicateOrReordered()).isEqualTo(1);
    }

    @Test
    @DisplayName("a sequence gap is counted -- QoS 1 cannot tell you about a message never sent")
    void gapIsCounted() {
        Instant now = Instant.now();
        registry.onTelemetry(telemetry(1, now));
        registry.onTelemetry(telemetry(4, now)); // 2 and 3 never arrived

        assertThat(only().missedMessages()).isEqualTo(2);
        assertThat(only().lastSequence()).isEqualTo(4);
    }

    @Test
    @DisplayName("the first message seen can never be a gap")
    void firstMessageIsNotAGap() {
        registry.onTelemetry(telemetry(500, Instant.now()));
        assertThat(only().missedMessages()).isZero();
    }

    @Test
    @DisplayName("freshness is judged on capture time, not arrival -- drained backlog is flagged")
    void backlogIsDetectedByCaptureTime() {
        registry.onTelemetry(telemetry(1, Instant.now().minus(Duration.ofMinutes(10))));
        assertThat(only().backloggedMessages()).isEqualTo(1);
        assertThat(only().maxIngestLagMillis()).isGreaterThan(Duration.ofMinutes(9).toMillis());
    }

    @Test
    @DisplayName("a robot with no recent heartbeat reads STALE, not UP")
    void staleWhenHeartbeatIsOld() {
        registry.onHealth(new RobotHealth("site1", "arm-01", HealthStatus.UP,
                Instant.now().minus(Duration.ofMinutes(5)), 100L, 10L, Map.of(), null));
        assertThat(only().status()).isEqualTo("STALE");
    }

    @Test
    @DisplayName("a Last Will beats the freshness heuristic")
    void lastWillWins() {
        registry.onHealth(new RobotHealth("site1", "gateway", HealthStatus.UP,
                Instant.now(), 100L, 10L, Map.of(), null));
        registry.onHealth(RobotHealth.lastWill("site1", "gateway"));
        assertThat(only().status()).isEqualTo("DOWN");
    }

    @Test
    @DisplayName("bridge notifications are decoded from mosquitto's 1/0 payload")
    void bridgeState() {
        registry.onBridgeState("site1", "1");
        assertThat(registry.bridgeSnapshot().get(0).connected()).isTrue();
        registry.onBridgeState("site1", "0");
        assertThat(registry.bridgeSnapshot().get(0).connected()).isFalse();
        assertThat(registry.bridgeSnapshot().get(0).transitions()).isEqualTo(1);
    }
}
