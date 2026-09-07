package com.jinternals.mqtt.cloud.config;

import com.jinternals.mqtt.cloud.protocol.Telemetry;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.cloud")
public class CloudProperties {

    /**
     * A robot with no heartbeat for this long is marked STALE.
     *
     * <p>Must be a comfortable multiple of the edge heartbeat interval. Too tight and every WAN
     * blip pages someone; too loose and a genuinely dead site goes unnoticed for an hour.
     */
    private Duration healthTimeout = Duration.ofSeconds(60);

    /**
     * Default lifetime stamped on commands that do not specify one, and the only time-based
     * expiry anywhere in this system. It is how long a queued command stays valid while a site is
     * offline; past it the cloud broker drops the message and the edge refuses to run it.
     *
     * <p>Telemetry and health carry no expiry at all, by design — see
     * {@code MqttConnection#publish}.
     */
    private Duration defaultCommandTtl = Duration.ofMinutes(15);

    /**
     * Telemetry arriving later than this after it was captured is counted as backlog — i.e. it was
     * almost certainly drained from a bridge queue rather than delivered live.
     *
     * <p>Kept separate from {@link #healthTimeout} on purpose. They are numerically similar and it
     * is tempting to reuse one for the other, but they answer different questions ("is this robot
     * alive?" versus "did this reading come from a queue?") and would drift apart the moment either
     * is tuned.
     */
    private Duration backlogLagThreshold = Duration.ofSeconds(30);

    public Duration getBacklogLagThreshold() {
        return backlogLagThreshold;
    }

    public void setBacklogLagThreshold(Duration backlogLagThreshold) {
        this.backlogLagThreshold = backlogLagThreshold;
    }

    /** How long completed command results are kept for retrieval over HTTP. */
    private Duration commandRetention = Duration.ofHours(1);

    public Duration getHealthTimeout() {
        return healthTimeout;
    }

    public void setHealthTimeout(Duration healthTimeout) {
        this.healthTimeout = healthTimeout;
    }

    public Duration getDefaultCommandTtl() {
        return defaultCommandTtl;
    }

    public void setDefaultCommandTtl(Duration defaultCommandTtl) {
        this.defaultCommandTtl = defaultCommandTtl;
    }

    public Duration getCommandRetention() {
        return commandRetention;
    }

    public void setCommandRetention(Duration commandRetention) {
        this.commandRetention = commandRetention;
    }
}
