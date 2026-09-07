package com.jinternals.mqtt.edge.config;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Site identity and the simulated robot inventory. */
@ConfigurationProperties(prefix = "app.edge")
public class EdgeProperties {

    /** {@code site1}, {@code site2}, ... Must match the bridge topic map on this site's broker. */
    private String siteId;

    /** Devices this gateway speaks for. In the real thing these come from a local inventory. */
    private List<String> robots = List.of();

    private Duration telemetryInterval = Duration.ofSeconds(5);

    private Duration healthInterval = Duration.ofSeconds(15);

    /**
     * How many recent command ids to remember for deduplication. QoS 1 across a bridge means
     * commands can be redelivered; this window is what makes execution idempotent — and picking is
     * exactly the domain where that matters, because a repeated PICK_ITEM puts a second unit in a
     * tote that was only supposed to get one.
     */
    private int commandDedupeWindow = 1000;

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String siteId) {
        this.siteId = siteId;
    }

    public List<String> getRobots() {
        return robots;
    }

    public void setRobots(List<String> robots) {
        this.robots = robots;
    }

    public Duration getTelemetryInterval() {
        return telemetryInterval;
    }

    public void setTelemetryInterval(Duration telemetryInterval) {
        this.telemetryInterval = telemetryInterval;
    }

    public Duration getHealthInterval() {
        return healthInterval;
    }

    public void setHealthInterval(Duration healthInterval) {
        this.healthInterval = healthInterval;
    }

    public int getCommandDedupeWindow() {
        return commandDedupeWindow;
    }

    public void setCommandDedupeWindow(int commandDedupeWindow) {
        this.commandDedupeWindow = commandDedupeWindow;
    }
}
