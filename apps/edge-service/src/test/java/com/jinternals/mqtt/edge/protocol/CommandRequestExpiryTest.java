package com.jinternals.mqtt.edge.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Expiry is the only time-based rule in the system and it exists to stop a command that queued
 * through an outage from executing hours late. These tests pin the boundary, because "off by one
 * comparison" here means either running stale commands or refusing valid ones.
 */
class CommandRequestExpiryTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private CommandRequest request(Instant expiresAt) {
        return new CommandRequest(
                "cmd-1", "site1", "arm-01", CommandType.PING, Map.of(), T0, expiresAt);
    }

    @Test
    @DisplayName("a command inside its window is executable")
    void notExpiredBeforeDeadline() {
        CommandRequest cmd = request(T0.plus(Duration.ofMinutes(15)));
        assertThat(cmd.isExpired(T0.plus(Duration.ofMinutes(14)))).isFalse();
    }

    @Test
    @DisplayName("expiry is exclusive: exactly at the deadline is still valid")
    void notExpiredExactlyAtDeadline() {
        Instant deadline = T0.plus(Duration.ofMinutes(15));
        assertThat(request(deadline).isExpired(deadline)).isFalse();
    }

    @Test
    @DisplayName("one instant past the deadline is expired")
    void expiredAfterDeadline() {
        Instant deadline = T0.plus(Duration.ofMinutes(15));
        assertThat(request(deadline).isExpired(deadline.plusMillis(1))).isTrue();
    }

    @Test
    @DisplayName("a command with no deadline never expires")
    void nullExpiryNeverExpires() {
        assertThat(request(null).isExpired(T0.plus(Duration.ofDays(365)))).isFalse();
    }
}
